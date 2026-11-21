//! Stage 4: person, sessions, day and category (rules.md "Events, in this order", steps 5-7).
//! Mirrors `pipeline/events.py` from `ev_person` to `sessions`.
use rayon::prelude::*;

use crate::events::{Event, PURCHASE};
use crate::raw::Row;
use crate::util::{hash_bytes, partition, FxMap};

pub const GAP_MS: i64 = 1_800_000;
pub const DAY_MS: i64 = 86_400_000;
pub const UNKNOWN: &[u8] = b"unknown";

/// A valid event with its person and category.
#[derive(Clone, Copy)]
pub struct PEvent<'a> {
    pub person: &'a [u8],
    pub ev: Event<'a>,
    pub category: &'a [u8],
}

#[derive(Clone, Copy)]
pub struct Session<'a> {
    pub person: &'a [u8],
    pub start: i64,
    pub day: i32,
    /// events[first .. first + n_events] of the same part
    pub first: u32,
    pub n_events: u32,
    pub revenue: i64,
    pub has_purchase: bool,
}

/// One hash partition by person: its events sorted by (person, ts) and its sessions in the same order.
pub struct Part<'a> {
    pub events: Vec<PEvent<'a>>,
    pub sessions: Vec<Session<'a>>,
}

pub fn day_of(ms: i64) -> i32 {
    ms.div_euclid(DAY_MS) as i32
}

pub fn product_map<'a>(products: &[Row<'a, 2>]) -> FxMap<&'a [u8], &'a [u8]> {
    let mut m = FxMap::with_capacity_and_hasher(products.len(), Default::default());
    for p in products {
        if let Some(id) = p[0] {
            m.entry(id).or_insert(p[1].unwrap_or(UNKNOWN));
        }
    }
    m
}

pub fn sessionize<'a>(
    ev: &[Event<'a>],
    pmap: &FxMap<&'a [u8], &'a [u8]>,
    products: &FxMap<&'a [u8], &'a [u8]>,
) -> Vec<Part<'a>> {
    let person = |u: &'a [u8]| -> &'a [u8] { pmap.get(u).copied().unwrap_or(u) };
    let parts = partition(ev.len(), |i| Some(hash_bytes(person(ev[i].user))));
    parts
        .into_par_iter()
        .map(|idx| {
            let mut events: Vec<PEvent<'a>> = idx
                .iter()
                .map(|&i| {
                    let e = ev[i as usize];
                    PEvent { person: person(e.user), ev: e, category: products.get(e.product).copied().unwrap_or(UNKNOWN) }
                })
                .collect();
            events.sort_unstable_by(|a, b| a.person.cmp(b.person).then(a.ev.ts.cmp(&b.ev.ts)));
            let mut sessions: Vec<Session<'a>> = Vec::new();
            for (i, e) in events.iter().enumerate() {
                let new = match sessions.last() {
                    Some(s) => s.person != e.person || e.ev.ts - events[i - 1].ev.ts > GAP_MS,
                    None => true,
                };
                if new {
                    sessions.push(Session {
                        person: e.person,
                        start: e.ev.ts,
                        day: day_of(e.ev.ts),
                        first: i as u32,
                        n_events: 0,
                        revenue: 0,
                        has_purchase: false,
                    });
                }
                let s = sessions.last_mut().unwrap();
                s.n_events += 1;
                if e.ev.kind == PURCHASE {
                    s.revenue += e.ev.price;
                    s.has_purchase = true;
                }
            }
            Part { events, sessions }
        })
        .collect()
}
