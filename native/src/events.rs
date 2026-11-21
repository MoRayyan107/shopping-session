//! Stage 2: events dedup, validation and the late filter (rules.md "Events, in this order", steps 1-4),
//! plus the person (step 5) and category (step 7). Mirrors `pipeline/events.py` up to `ev_person`.
//!
//! One parallel pass over the raw bytes splits each line, validates it (validity depends only on the copy
//! itself) and writes a compact record; each ~4 MB piece keeps its records in one array ordered by
//! hash(event_id) bucket. Each bucket then dedups: a unique event_id hash needs nothing more; copies (or a
//! hash collision) are re-split and ordered exactly as the oracle does. Kept valid events go to buckets by
//! hash(person), ready for sessions. Strings are 8-byte `Span`s into the input files.
use std::cmp::Ordering;

use crate::csv::{for_each_row, split_line};
use crate::key::Key;
use crate::links::{PersonTable, PREFETCH_ROWS};
use crate::raw::{pieces_with_file, Mapped, Row};
use crate::span::{Span, Store};
use crate::ts::{parse_price, parse_ts};
use crate::util::{cmp_nulls_last, dedup_find_into, for_each_in_order, Groups, prof, Chunks, FxMap, Scatter, NPARTS, PART_BITS};

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

    /// The category id of a product.
    #[inline]
    pub fn of(&self, product: &[u8]) -> u32 {
        self.product.get(product).copied().unwrap_or(self.unknown)
    }
}

/// price (at most 10,000,000 < 2^24) in the low 24 bits, the event type in the top 8.
#[inline(always)]
fn pack_pk(price: u32, kind: u8) -> u32 {
    debug_assert!(price < 1 << 24);
    ((kind as u32) << 24) | price
}

/// One valid event, mapped to its person (32 bytes). `line` is its raw line (used for dumps). `cat` is only
/// set for purchases.
#[derive(Clone, Copy)]
pub struct Event {
    /// the person's exact key; its bucket is the top bits of `person.hash()`
    pub person: Key,
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
    pub fn test(person: Key, ts: i64, price: u32, kind: u8) -> Event {
        Event { person, ts, line: Span::EMPTY, price_kind: pack_pk(price, kind), cat: 0 }
    }
}

/// One well-formed line with a non-empty event_id, validated (48 bytes). Type INVALID when the copy fails.
#[derive(Clone, Copy)]
pub struct Rec {
    /// the event_id's exact key (its hash picks the dedup bucket)
    id: Key,
    /// the user ID's exact key (the default key when the copy is invalid)
    user: Key,
    line: Span,
    ts: i64,
    /// ingest_ts in ms when it is an F1 timestamp (canonical: F1 strings order exactly as their times), else
    /// NO_INGEST, which sends the copies of this event_id to the exact line-by-line resolution
    ingest: i64,
    price_kind: u32,
    cat: u32,
}

const NO_INGEST: i64 = i64::MAX;

/// ingest_ts in ms when it is a valid F1 timestamp, else NO_INGEST.
#[inline]
fn f1_ms(s: Option<&[u8]>) -> i64 {
    match s {
        Some(s) if s.len() == 20 && s[19] == b'Z' => parse_ts(s).unwrap_or(NO_INGEST),
        _ => NO_INGEST,
    }
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

/// Validation and the late filter for one copy: (user, product, ts, price, kind). `ingest` is the line's
/// `f1_ms` (any other valid form is parsed here).
#[inline]
fn validate<'a>(r: &Row<'a, 7>, ws: i64, we: i64, ingest: i64) -> Option<(&'a [u8], &'a [u8], i64, u32, u8)> {
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
    let ingest = if ingest != NO_INGEST { ingest } else { parse_ts(r[3]?)? };
    if ingest - ts > LATE_MS {
        return None;
    }
    Some((user, product, ts, price, kind))
}

pub struct Built {
    /// valid events, bucketed by the top bits of hash(person)
    pub by_person: Chunks<Event>,
    /// distinct event_ids
    pub kept: usize,
}

#[inline]
fn bucket(h: u64) -> usize {
    (h >> (64 - PART_BITS)) as usize
}

/// Pass 1: split + validate every line; each record goes straight to its hash(event_id) bucket.
pub fn split<'a>(files: &'a Mapped, first: u32, store: &Store<'a>, cats: &Categories<'_>, ws: i64, we: i64) -> Chunks<Rec> {
    let mut t = std::time::Instant::now();
    let scatters = for_each_in_order(&pieces_with_file(files, first), Scatter::default, |sc: &mut Scatter<Rec>, &(file, piece)| {
            crate::raw::note_taken(piece.len());
            for_each_row::<7, _>(piece, |line, f| {
                let Some(id) = f[0] else { return };
                let id = store.key(file, id);
                let line_sp = store.span(file, line);
                let ingest = f1_ms(f[3]);
                let rec = match validate(f, ws, we, ingest) {
                    Some((user, product, ts, price, kind)) => Rec {
                        id,
                        user: store.key(file, user),
                        line: line_sp,
                        ts,
                        ingest,
                        price_kind: pack_pk(price, kind),
                        // only purchases need a category (top_category sums purchase prices); the dumps
                        // recompute it from the line (`Categories::of`)
                        cat: if kind == PURCHASE { cats.of(product) } else { 0 },
                    },
                    None => Rec {
                        id,
                        user: Key::default(),
                        line: line_sp,
                        ts: 0,
                        ingest,
                        price_kind: pack_pk(0, INVALID),
                        cat: 0,
                    },
                };
                sc.push(bucket(rec.id.hash()), rec);
            });
        });
    prof("events: split + validate into buckets", &mut t);
    Chunks::new(scatters)
}

/// Pass 2: dedup each id bucket (each bucket's records are freed once it is done); kept valid events go
/// straight to their hash(person) bucket.
pub fn dedup(by_id: Chunks<Rec>, ptab: &PersonTable, store: &Store<'_>) -> Built {
    let mut t = std::time::Instant::now();
    let ids: Vec<usize> = (0..NPARTS).collect();
    type State = (Vec<u32>, Scatter<Event>, usize, Groups, Vec<(Rec, usize)>);
    let states = for_each_in_order(
        &ids,
        || (Vec::new(), Scatter::default(), 0usize, Groups::default(), Vec::new()),
        |(table, events, kept, groups, gbuf): &mut State, &b| {
                // the bucket's chunks as they are (no copy into one buffer); freed at the end of this bucket
                let recs = by_id.take_rows(b);
                let emit = |r: &Rec, events: &mut Scatter<Event>| {
                    if (r.price_kind >> 24) as u8 != INVALID {
                        let person = ptab.lookup(r.user);
                        events.push(
                            bucket(person.hash()),
                            Event { person, ts: r.ts, line: r.line, price_kind: r.price_kind, cat: r.cat },
                        );
                    }
                };
                dedup_find_into(recs.len(), recs.iter().map(|r| r.id.hash()), table, groups);
                // unique ids in memory order (sequential reads, chunk by chunk), then the groups of copies;
                // the person table is random access: its filter word is requested 2D rows ahead, its slot D
                // (within the chunk)
                const D: usize = PREFETCH_ROWS;
                let mut i = 0;
                for c in recs.chunks() {
                    for j in 0..c.len() {
                        if let Some(r) = c.get(j + 2 * D) {
                            ptab.prefetch_bits(r.user);
                        }
                        if let Some(r) = c.get(j + D) {
                            ptab.prefetch_slot(r.user);
                        }
                        if groups.alone[i] {
                            *kept += 1;
                            emit(&c[j], events);
                        }
                        i += 1;
                    }
                }
                for gi in 0..groups.len() {
                    // a truncated-hash group: runs of one event_id (equal keys <=> equal IDs) by ingest_ts;
                    // its rows copied out (copies are few) with their row numbers
                    gbuf.clear();
                    gbuf.extend(groups.group(gi).iter().map(|&k| (*recs.get(k as usize), k as usize)));
                    let g = &mut *gbuf;
                    g.sort_unstable_by_key(|(r, k)| (r.id, r.ingest, *k));
                    let mut a = 0;
                    while a < g.len() {
                        let mut z = a + 1;
                        while z < g.len() && g[z].0.id == g[a].0.id {
                            z += 1;
                        }
                        let run = &g[a..z];
                        a = z;
                        *kept += 1;
                        // the oracle keeps the first copy by (ingest_ts as a string, then the other columns):
                        // when every copy's ingest_ts is F1 and the smallest is unique, that is run[0]
                        let unique_first = run.iter().all(|(r, _)| r.ingest != NO_INGEST)
                            && (run.len() == 1 || run[0].0.ingest != run[1].0.ingest);
                        if unique_first {
                            emit(&run[0].0, events);
                            continue;
                        }
                        // otherwise the exact order from the lines themselves
                        let first = run
                            .iter()
                            .map(|(r, k)| {
                                let mut f = [None; 7];
                                split_line(store.get(r.line), &mut f);
                                (f, *k, r)
                            })
                            .min_by(|x, y| copy_order(&x.0, &y.0).then(x.1.cmp(&y.1)))
                            .unwrap();
                        emit(first.2, events);
                    }
                }
        },
    );
    drop(by_id);
    prof("events: dedup + person", &mut t);
    let kept = states.iter().map(|s| s.2).sum();
    let by_person = Chunks::new(states.into_iter().map(|s| s.1).collect());
    Built { by_person, kept }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn record_sizes() {
        assert_eq!(std::mem::size_of::<Rec>(), 48);
        assert_eq!(std::mem::size_of::<Event>(), 32);
    }
    #[test]
    fn f1_ingest_only() {
        assert_eq!(f1_ms(Some(b"2026-10-03T23:58:10Z")), 1791071890000);
        assert_eq!(f1_ms(Some(b"2026-10-03T19:58:10-04:00")), NO_INGEST);
        assert_eq!(f1_ms(Some(b"1791071890000")), NO_INGEST);
        assert_eq!(f1_ms(Some(b"2026-02-30T00:00:00Z")), NO_INGEST);
        assert_eq!(f1_ms(None), NO_INGEST);
        // F1 strings order exactly as their times (fixed width, most significant first)
        let a: &[u8] = b"2026-10-03T23:58:10Z";
        let b: &[u8] = b"2026-10-04T00:00:00Z";
        assert_eq!(a.cmp(b), f1_ms(Some(a)).cmp(&f1_ms(Some(b))));
    }
    #[test]
    fn price_kind_packing() {
        let e = Event::test(Key::default(), 0, 10_000_000, PURCHASE);
        assert_eq!((e.price(), e.kind()), (10_000_000, PURCHASE));
        let e = Event::test(Key::default(), 0, 0, VIEW);
        assert_eq!((e.price(), e.kind()), (0, VIEW));
        assert_eq!((pack_pk(0, INVALID) >> 24) as u8, INVALID);
    }
}
