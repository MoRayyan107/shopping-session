//! Stage 4: sessions and day (rules.md "Events, in this order", steps 6-7).
//! Mirrors `pipeline/events.py` from `sess_ts` to `sessions`.
use rayon::prelude::*;

use crate::events::{Event, PURCHASE};

pub const GAP_MS: i64 = 1_800_000;
pub const DAY_MS: i64 = 86_400_000;

#[derive(Clone, Copy)]
pub struct Session<'a> {
    pub person: &'a [u8],
    pub phash: u64,
    pub start: i64,
    pub day: i32,
    /// events[first .. first + n_events] of the same part
    pub first: u32,
    pub n_events: u32,
    pub revenue: i64,
    pub has_purchase: bool,
}

/// One hash partition by person: its events grouped by person and sorted by ts, and its sessions in order.
pub struct Part<'a> {
    pub events: Vec<Event<'a>>,
    pub sessions: Vec<Session<'a>>,
}

pub fn day_of(ms: i64) -> i32 {
    ms.div_euclid(DAY_MS) as i32
}

/// Sort a bucket so each person's events are contiguous and in ts order. Sorting is by (hash, ts); if two
/// different persons share a 64-bit hash (checked), the bucket is sorted again by (hash, person, ts).
pub fn group_by_person<T, F>(v: &mut [T], key: F)
where
    F: Fn(&T) -> (u64, &[u8], i64, &[u8]) + Copy,
{
    v.sort_unstable_by(|a, b| {
        let (ha, _, ta, xa) = key(a);
        let (hb, _, tb, xb) = key(b);
        ha.cmp(&hb).then(ta.cmp(&tb)).then(xa.cmp(xb))
    });
    let collision = v.windows(2).any(|w| {
        let (ha, pa, _, _) = key(&w[0]);
        let (hb, pb, _, _) = key(&w[1]);
        ha == hb && pa != pb
    });
    if collision {
        v.sort_unstable_by(|a, b| {
            let (ha, pa, ta, xa) = key(a);
            let (hb, pb, tb, xb) = key(b);
            ha.cmp(&hb).then(pa.cmp(pb)).then(ta.cmp(&tb)).then(xa.cmp(xb))
        });
    }
}

pub fn sessionize(by_person: Vec<Vec<Event<'_>>>) -> Vec<Part<'_>> {
    by_person
        .into_par_iter()
        .map(|mut events| {
            group_by_person(&mut events, |e| (e.phash, e.person, e.ts, &[]));
            let mut sessions: Vec<Session<'_>> = Vec::with_capacity(events.len() / 4);
            for (i, e) in events.iter().enumerate() {
                let new = match sessions.last() {
                    Some(s) => s.phash != e.phash || s.person != e.person || e.ts - events[i - 1].ts > GAP_MS,
                    None => true,
                };
                if new {
                    sessions.push(Session {
                        person: e.person,
                        phash: e.phash,
                        start: e.ts,
                        day: day_of(e.ts),
                        first: i as u32,
                        n_events: 0,
                        revenue: 0,
                        has_purchase: false,
                    });
                }
                let s = sessions.last_mut().unwrap();
                s.n_events += 1;
                if e.kind == PURCHASE {
                    s.revenue += e.price as i64;
                    s.has_purchase = true;
                }
            }
            Part { events, sessions }
        })
        .collect()
}
