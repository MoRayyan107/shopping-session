//! Stage 2: events dedup, validation and the late filter (rules.md "Events, in this order", steps 1-4),
//! plus the person (step 5) and category (step 7). Mirrors `pipeline/events.py` up to `ev_person`.
//!
//! One parallel pass over the raw bytes splits each line, validates it (validity depends only on the copy
//! itself) and writes a compact record; each ~4 MB piece keeps its records in one array ordered by
//! hash(event_id) bucket. Each bucket then dedups: a unique event_id hash needs nothing more; copies (or a
//! hash collision) are re-split and ordered exactly as the oracle does. Kept valid events go to buckets by
//! hash(person), ready for sessions. Strings are 8-byte `Span`s into the input files.
use std::cmp::Ordering;

use rayon::prelude::*;

use crate::csv::{lines, split_line};
use crate::links::PersonTable;
use crate::raw::{pieces_with_file, Mapped, Row};
use crate::span::{Span, Store};
use crate::ts::{parse_price, parse_ts};
use crate::util::{cmp_nulls_last, dedup_groups, dedup_order, hash_bytes, map_in_order, prof, sort_by_bucket, Buckets, FxMap, PART_BITS};

pub const VIEW: u8 = 0;
pub const CART: u8 = 1;
pub const PURCHASE: u8 = 2;
const INVALID: u8 = 255;
pub const TYPE_NAMES: [&[u8]; 3] = [b"view", b"cart", b"purchase"];

const LATE_MS: i64 = 86_400_000;
pub const UNKNOWN: &[u8] = b"unknown";

/// Categories interned as u32 ids, and product -> category id. A product missing from products.csv (or with
/// an empty category) gets the id of the name "unknown", as the oracle's COALESCE(category, 'unknown') does.
pub struct Categories<'a> {
    pub names: Vec<&'a [u8]>,
    pub product: FxMap<&'a [u8], u32>,
    pub unknown: u32,
}

impl<'a> Categories<'a> {
    pub fn new(products: &[Row<'a, 2>]) -> Categories<'a> {
        let mut names: Vec<&'a [u8]> = Vec::new();
        let mut idx: FxMap<&'a [u8], u32> = FxMap::default();
        let mut intern = |s: &'a [u8], names: &mut Vec<&'a [u8]>| {
            *idx.entry(s).or_insert_with(|| {
                names.push(s);
                (names.len() - 1) as u32
            })
        };
        let mut product = FxMap::with_capacity_and_hasher(products.len(), Default::default());
        for p in products {
            if let Some(id) = p[0] {
                if !product.contains_key(id) {
                    let c = intern(p[1].unwrap_or(UNKNOWN), &mut names);
                    product.insert(id, c);
                }
            }
        }
        let unknown = intern(UNKNOWN, &mut names);
        Categories { names, product, unknown }
    }
}

/// price (at most 10,000,000 < 2^24) in the low 24 bits, the event type in the top 8.
#[inline(always)]
fn pack_pk(price: u32, kind: u8) -> u32 {
    debug_assert!(price < 1 << 24);
    ((kind as u32) << 24) | price
}

/// One valid event, mapped to its person (40 bytes). `line` is its raw line (used for dumps).
#[derive(Clone, Copy)]
pub struct Event {
    pub person: Span,
    pub phash: u64,
    pub ts: i64,
    pub line: Span,
    price_kind: u32,
    pub cat: u32,
}

impl Event {
    #[inline(always)]
    pub fn kind(&self) -> u8 {
        (self.price_kind >> 24) as u8
    }
    #[inline(always)]
    pub fn price(&self) -> u32 {
        self.price_kind & 0x00FF_FFFF
    }
    /// The raw fields of the event's line (always well-formed: it was parsed before).
    pub fn row<'a>(&self, store: &Store<'a>) -> Row<'a, 7> {
        let mut f = [None; 7];
        split_line(store.get(self.line), &mut f);
        f
    }
    #[cfg(test)]
    pub fn test(person: Span, phash: u64, ts: i64, price: u32, kind: u8) -> Event {
        Event { person, phash, ts, line: Span::EMPTY, price_kind: pack_pk(price, kind), cat: 0 }
    }
}

/// One well-formed line with a non-empty event_id, validated (48 bytes). Type INVALID when the copy fails.
#[derive(Clone, Copy)]
pub struct Rec {
    idhash: u64,
    uhash: u64,
    line: Span,
    user: Span,
    ts: i64,
    price_kind: u32,
    cat: u32,
}

/// The oracle's dedup order within one event_id: ingest_ts (raw string), then the other columns, NULLs last.
#[inline]
fn copy_order(a: &Row<'_, 7>, b: &Row<'_, 7>) -> Ordering {
    for col in [3usize, 1, 2, 4, 5, 6] {
        let o = cmp_nulls_last(a[col], b[col]);
        if o != Ordering::Equal {
            return o;
        }
    }
    Ordering::Equal
}

/// Validation and the late filter for one copy: (user, product, ts, price, kind).
#[inline]
fn validate<'a>(r: &Row<'a, 7>, ws: i64, we: i64) -> Option<(&'a [u8], &'a [u8], i64, u32, u8)> {
    let user = r[1]?;
    let product = r[5]?;
    let ts = parse_ts(r[2]?)?;
    if ts < ws || ts >= we {
        return None;
    }
    let kind = match r[4]? {
        b"view" => VIEW,
        b"cart" => CART,
        b"purchase" => PURCHASE,
        _ => return None,
    };
    let price = if kind == PURCHASE { parse_price(r[6]?)? as u32 } else { 0 };
    let ingest = parse_ts(r[3]?)?;
    if ingest - ts > LATE_MS {
        return None;
    }
    Some((user, product, ts, price, kind))
}

pub struct Built {
    /// valid events, bucketed by the top bits of hash(person)
    pub by_person: Vec<Vec<Event>>,
    /// distinct event_ids
    pub kept: usize,
}

#[inline]
fn bucket(h: u64) -> usize {
    (h >> (64 - PART_BITS)) as usize
}

/// Pass 1: split + validate every line; each piece's records ordered by hash(event_id) bucket.
pub fn split<'a>(files: &'a Mapped, first: u32, store: &Store<'a>, cats: &Categories<'_>, ws: i64, we: i64) -> Buckets<Rec> {
    let mut t = std::time::Instant::now();
    let blocks: Vec<(Vec<Rec>, Vec<u32>)> = map_in_order(&pieces_with_file(files, first), Vec::new, |recs: &mut Vec<Rec>, &(file, piece)| {
            crate::raw::note_taken(piece.len());
            recs.clear();
            let mut f: Row<'_, 7> = [None; 7];
            for line in lines(piece) {
                if !split_line(line, &mut f) {
                    continue;
                }
                let Some(id) = f[0] else { continue };
                let idhash = hash_bytes(id);
                let line_sp = store.span(file, line);
                let rec = match validate(&f, ws, we) {
                    Some((user, product, ts, price, kind)) => Rec {
                        idhash,
                        uhash: hash_bytes(user),
                        line: line_sp,
                        user: store.span(file, user),
                        ts,
                        price_kind: pack_pk(price, kind),
                        cat: cats.product.get(product).copied().unwrap_or(cats.unknown),
                    },
                    None => Rec {
                        idhash,
                        uhash: 0,
                        line: line_sp,
                        user: Span::EMPTY,
                        ts: 0,
                        price_kind: pack_pk(0, INVALID),
                        cat: 0,
                    },
                };
                recs.push(rec);
            }
            sort_by_bucket(recs, |r| bucket(r.idhash))
        });
    prof("events: split + validate into blocks", &mut t);
    Buckets::new(blocks)
}

/// Pass 2: dedup each id bucket; kept valid events go to buckets by hash(person). The records are freed
/// before the person buckets are gathered.
pub fn dedup(by_id: Buckets<Rec>, ptab: &PersonTable, store: &Store<'_>) -> Built {
    let nparts = 1usize << PART_BITS;
    let mut t = std::time::Instant::now();
    let out: Vec<(usize, (Vec<Event>, Vec<u32>))> = (0..nparts)
        .into_par_iter()
        .map_init(
            || (Vec::new(), Vec::new()),
            |(recs, events): &mut (Vec<Rec>, Vec<Event>), b| {
                recs.clear();
                events.clear();
                by_id.gather(b, recs);
                let order = dedup_order(recs.len(), |i| recs[i].idhash);
                let mut kept = 0usize;
                let emit = |r: &Rec, events: &mut Vec<Event>| {
                    if (r.price_kind >> 24) as u8 != INVALID {
                        let (person, phash) = ptab.lookup(r.uhash, r.user, store);
                        events.push(Event { person, phash, ts: r.ts, line: r.line, price_kind: r.price_kind, cat: r.cat });
                    }
                };
                let (alone, groups) = dedup_groups(&order);
                // unique ids in memory order (sequential reads), then the groups of copies
                for (i, r) in recs.iter().enumerate() {
                    if alone[i] {
                        kept += 1;
                        emit(r, events);
                    }
                }
                for grp in &groups {
                    // copies of one id (or a hash collision): order by (id, the oracle's copy order)
                    let mut g: Vec<(Row<'_, 7>, usize)> = grp
                        .iter()
                        .map(|&k| {
                            let mut f = [None; 7];
                            split_line(store.get(recs[k].line), &mut f);
                            (f, k)
                        })
                        .collect();
                    g.sort_unstable_by(|a, b| a.0[0].cmp(&b.0[0]).then_with(|| copy_order(&a.0, &b.0)));
                    for (n, (row, k)) in g.iter().enumerate() {
                        if n == 0 || g[n - 1].0[0] != row[0] {
                            kept += 1;
                            emit(&recs[*k], events);
                        }
                    }
                }
                (kept, sort_by_bucket(events, |e| bucket(e.phash)))
            },
        )
        .collect();
    drop(by_id);
    prof("events: sort + dedup + person", &mut t);
    let kept = out.iter().map(|o| o.0).sum();
    let by_person = Buckets::new(out.into_iter().map(|o| o.1).collect()).into_vecs();
    prof("events: gather person buckets", &mut t);
    Built { by_person, kept }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn record_sizes() {
        assert_eq!(std::mem::size_of::<Rec>(), 48);
        assert_eq!(std::mem::size_of::<Event>(), 40);
    }
    #[test]
    fn price_kind_packing() {
        let e = Event::test(Span::EMPTY, 0, 0, 10_000_000, PURCHASE);
        assert_eq!((e.price(), e.kind()), (10_000_000, PURCHASE));
        let e = Event::test(Span::EMPTY, 0, 0, 0, VIEW);
        assert_eq!((e.price(), e.kind()), (0, VIEW));
        assert_eq!((pack_pk(0, INVALID) >> 24) as u8, INVALID);
    }
}
