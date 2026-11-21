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

#[cfg(test)]
mod tests {
    use super::*;

    fn ev<'a>(person: &'a [u8], phash: u64, ts: i64) -> Event<'a> {
        Event { person, phash, ts, line: b"", price: 0, cat: 0, kind: 0 }
    }

    #[test]
    fn colliding_person_hashes_never_merge() {
        // two persons with the same (forced) hash, interleaved in time, plus one other person
        let v = vec![ev(b"b", 7, 30), ev(b"a", 7, 10), ev(b"c", 1, 5), ev(b"b", 7, 20), ev(b"a", 7, 40), ev(b"a", 7, 25)];
        let parts = sessionize(vec![v]);
        let p = &parts[0];
        // each person's events contiguous and in ts order
        let seq: Vec<(&[u8], i64)> = p.events.iter().map(|e| (e.person, e.ts)).collect();
        assert_eq!(seq, vec![(&b"c"[..], 5), (b"a", 10), (b"a", 25), (b"a", 40), (b"b", 20), (b"b", 30)]);
        // gaps are ms here, far under 30 minutes: one session per person, never one across persons
        let s: Vec<(&[u8], u32)> = p.sessions.iter().map(|s| (s.person, s.n_events)).collect();
        assert_eq!(s, vec![(&b"c"[..], 1), (b"a", 3), (b"b", 2)]);
    }

    #[test]
    fn gap_rule() {
        let v = vec![ev(b"a", 1, 0), ev(b"a", 1, GAP_MS), ev(b"a", 1, 2 * GAP_MS + 1), ev(b"a", 1, 2 * GAP_MS + 1)];
        let parts = sessionize(vec![v]);
        let s: Vec<(i64, u32)> = parts[0].sessions.iter().map(|s| (s.start, s.n_events)).collect();
        // a gap of exactly 30 minutes stays in the session; equal timestamps always share one
        assert_eq!(s, vec![(0, 2), (2 * GAP_MS + 1, 2)]);
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
