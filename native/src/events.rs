//! Stage 2: events dedup, validation and the late filter (rules.md "Events, in this order", steps 1-4).
//! Mirrors `pipeline/events.py` up to `ev_valid`.
use std::cmp::Ordering;

use rayon::prelude::*;

use crate::raw::Row;
use crate::ts::{parse_price, parse_ts};
use crate::util::{cmp_nulls_last, hash_bytes, partition, FxMap};

pub const VIEW: u8 = 0;
pub const CART: u8 = 1;
pub const PURCHASE: u8 = 2;
pub const TYPE_NAMES: [&[u8]; 3] = [b"view", b"cart", b"purchase"];

const LATE_MS: i64 = 86_400_000;

/// One event after validation: borrowed IDs, parsed time, type, and the price (purchases only, else 0).
#[derive(Clone, Copy)]
pub struct Event<'a> {
    pub user: &'a [u8],
    pub product: &'a [u8],
    pub ts: i64,
    pub price: i64,
    pub kind: u8,
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

/// Indices of the kept copy of every event_id (rows with an empty event_id are dropped first).
pub fn dedup(rows: &[Row<'_, 7>]) -> Vec<u32> {
    let parts = partition(rows.len(), |i| rows[i][0].map(hash_bytes));
    let kept: Vec<Vec<u32>> = parts
        .par_iter()
        .map(|idx| {
            let mut best: FxMap<&[u8], u32> = FxMap::with_capacity_and_hasher(idx.len(), Default::default());
            for &i in idx {
                let r = &rows[i as usize];
                best.entry(r[0].unwrap())
                    .and_modify(|b| {
                        if copy_order(r, &rows[*b as usize]) == Ordering::Less {
                            *b = i;
                        }
                    })
                    .or_insert(i);
            }
            best.into_values().collect()
        })
        .collect();
    let mut out: Vec<u32> = kept.into_iter().flatten().collect();
    out.par_sort_unstable(); // deterministic order (file order)
    out
}

/// Validate the kept copies and drop late events.
pub fn validate<'a>(rows: &[Row<'a, 7>], kept: &[u32], ws: i64, we: i64) -> Vec<Event<'a>> {
    kept.par_iter()
        .filter_map(|&i| {
            let r = &rows[i as usize];
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
            let price = if kind == PURCHASE { parse_price(r[6]?)? } else { 0 };
            let ingest = parse_ts(r[3]?)?;
            if ingest - ts > LATE_MS {
                return None;
            }
            Some(Event { user, product, ts, price, kind })
        })
        .collect()
}
