//! Stage 2: events dedup, validation and the late filter (rules.md "Events, in this order", steps 1-4),
//! plus the person (step 5) and category (step 7). Mirrors `pipeline/events.py` up to `ev_person`.
//!
//! One parallel pass over the raw bytes splits each line, validates it (validity depends only on the copy
//! itself) and writes a compact record into a bucket by hash(event_id). Each bucket then dedups: a unique
//! event_id hash needs nothing more; copies (or a hash collision) are re-split and ordered exactly as the
//! oracle does. Kept valid events go to buckets by hash(person), ready for sessions.
use std::cmp::Ordering;

use rayon::prelude::*;

use crate::csv::{lines, split_line};
use crate::links::PersonTable;
use crate::raw::{pieces, Mapped, Row};
use crate::ts::{parse_price, parse_ts};
use crate::util::{cmp_nulls_last, hash_bytes, transpose_concat, FxMap, PART_BITS};

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

/// One valid event, mapped to its person. `line` is its raw line (used for dumps).
#[derive(Clone, Copy)]
pub struct Event<'a> {
    pub person: &'a [u8],
    pub phash: u64,
    pub ts: i64,
    pub line: &'a [u8],
    pub price: u32,
    pub cat: u32,
    pub kind: u8,
}

impl<'a> Event<'a> {
    /// The raw fields of the event's line (always well-formed: it was parsed before).
    pub fn row(&self) -> Row<'a, 7> {
        let mut f = [None; 7];
        split_line(self.line, &mut f);
        f
    }
}

/// One well-formed line with a non-empty event_id, validated. `kind == INVALID` when the copy fails.
#[derive(Clone, Copy)]
pub struct Rec<'a> {
    idhash: u64,
    uhash: u64,
    user: &'a [u8],
    line: &'a [u8],
    ts: i64,
    price: u32,
    cat: u32,
    kind: u8,
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

pub struct Built<'a> {
    /// valid events, bucketed by the top bits of hash(person)
    pub by_person: Vec<Vec<Event<'a>>>,
    /// distinct event_ids
    pub kept: usize,
}

#[inline]
fn bucket(h: u64) -> usize {
    (h >> (64 - PART_BITS)) as usize
}

/// Pass 1: split + validate every line into buckets by hash(event_id).
pub fn split<'a>(files: &'a Mapped, cats: &Categories<'a>, ws: i64, we: i64) -> Vec<Vec<Rec<'a>>> {
    let nparts = 1usize << PART_BITS;
    let blocks: Vec<Vec<Vec<Rec<'a>>>> = pieces(files)
        .par_iter()
        .map(|piece| {
            let mut out: Vec<Vec<Rec<'a>>> = (0..nparts).map(|_| Vec::with_capacity(piece.len() / 80 / nparts * 2)).collect();
            let mut f: Row<'a, 7> = [None; 7];
            for line in lines(piece) {
                if !split_line(line, &mut f) {
                    continue;
                }
                let Some(id) = f[0] else { continue };
                let idhash = hash_bytes(id);
                let rec = match validate(&f, ws, we) {
                    Some((user, product, ts, price, kind)) => Rec {
                        idhash,
                        uhash: hash_bytes(user),
                        user,
                        line,
                        ts,
                        price,
                        cat: cats.product.get(product).copied().unwrap_or(cats.unknown),
                        kind,
                    },
                    None => Rec { idhash, uhash: 0, user: &[], line, ts: 0, price: 0, cat: 0, kind: INVALID },
                };
                out[bucket(idhash)].push(rec);
            }
            out
        })
        .collect();
    transpose_concat(blocks)
}

/// Pass 2: dedup each id bucket; kept valid events go to buckets by hash(person).
pub fn dedup<'a>(by_id: Vec<Vec<Rec<'a>>>, ptab: &PersonTable<'a>) -> Built<'a> {
    let nparts = 1usize << PART_BITS;
    let out: Vec<(usize, Vec<Vec<Event<'a>>>)> = by_id
        .into_par_iter()
        .map(|mut recs| {
            recs.sort_unstable_by_key(|r| r.idhash);
            let mut buckets: Vec<Vec<Event<'a>>> =
                (0..nparts).map(|_| Vec::with_capacity(recs.len() / nparts + 16)).collect();
            let mut kept = 0usize;
            let emit = |r: &Rec<'a>, buckets: &mut Vec<Vec<Event<'a>>>| {
                if r.kind != INVALID {
                    let (person, phash) = ptab.lookup(r.uhash, r.user);
                    buckets[bucket(phash)].push(Event {
                        person,
                        phash,
                        ts: r.ts,
                        line: r.line,
                        price: r.price,
                        cat: r.cat,
                        kind: r.kind,
                    });
                }
            };
            let mut i = 0;
            while i < recs.len() {
                let mut j = i + 1;
                while j < recs.len() && recs[j].idhash == recs[i].idhash {
                    j += 1;
                }
                if j == i + 1 {
                    kept += 1;
                    emit(&recs[i], &mut buckets);
                } else {
                    // copies of one event_id (or a hash collision): order by (event_id, the oracle's copy order)
                    let mut g: Vec<(Row<'a, 7>, usize)> = (i..j)
                        .map(|k| {
                            let mut f = [None; 7];
                            split_line(recs[k].line, &mut f);
                            (f, k)
                        })
                        .collect();
                    g.sort_unstable_by(|a, b| a.0[0].cmp(&b.0[0]).then_with(|| copy_order(&a.0, &b.0)));
                    for (n, (row, k)) in g.iter().enumerate() {
                        if n == 0 || g[n - 1].0[0] != row[0] {
                            kept += 1;
                            emit(&recs[*k], &mut buckets);
                        }
                    }
                }
                i = j;
            }
            (kept, buckets)
        })
        .collect();
    let kept = out.iter().map(|o| o.0).sum();
    Built { by_person: transpose_concat(out.into_iter().map(|o| o.1).collect()), kept }
}
