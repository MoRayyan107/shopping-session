//! Stage 4: sessions and day (rules.md "Events, in this order", steps 6-7).
//! Mirrors `pipeline/events.py` from `sess_ts` to `sessions`.
use rayon::prelude::*;

use crate::events::{Event, PURCHASE};
use crate::key::Key;
use crate::util::{Chunks, NPARTS};

pub const GAP_MS: i64 = 1_800_000;
pub const DAY_MS: i64 = 86_400_000;

/// One session (40 bytes).
#[derive(Clone, Copy)]
pub struct Session {
    pub person: Key,
    pub start: i64,
    pub revenue: i64,
    /// events[first .. first + n_events] of the same part
    pub first: u32,
    pub n_events: u32,
    pub day: i32,
    pub has_purchase: bool,
}

/// One hash partition by person: its events grouped by person and sorted by ts, and its sessions in order.
/// `events` is emptied once user_daily is built (nothing later needs it).
pub struct Part {
    pub events: Vec<Event>,
    pub sessions: Vec<Session>,
}

pub fn day_of(ms: i64) -> i32 {
    ms.div_euclid(DAY_MS) as i32
}

/// Sort a bucket so each person's rows are contiguous and in ts order: by (person key, ts, tie-break bytes).
/// Keys are exact (equal keys <=> equal IDs), so this groups persons exactly.
pub fn group_by_person<'a, T, F>(v: &mut [T], key: F)
where
    F: Fn(&T) -> (Key, i64, &'a [u8]) + Copy,
{
    v.sort_unstable_by(|a, b| {
        let (pa, ta, xa) = key(a);
        let (pb, tb, xb) = key(b);
        pa.cmp(&pb).then(ta.cmp(&tb)).then_with(|| xa.cmp(xb))
    });
}

/// Every person bucket taken out of `by_person` (freed as it goes), sorted and cut into sessions; parts in
/// bucket order.
pub fn sessionize(by_person: &Chunks<Event>) -> Vec<Part> {
    (0..NPARTS)
        .into_par_iter()
        .map(|b| {
            let mut events = Vec::new();
            by_person.take(b, &mut events);
            sessionize_part(events)
        })
        .collect()
}

/// One person bucket's events, sorted and cut into sessions.
pub fn sessionize_part(mut events: Vec<Event>) -> Part {
            group_by_person(&mut events, |e| (e.person, e.ts, &[]));
            let mut sessions: Vec<Session> = Vec::with_capacity(events.len() / 4);
            for (i, e) in events.iter().enumerate() {
                let new = match sessions.last() {
                    Some(s) => {
                        s.person != e.person || e.ts - events[i - 1].ts > GAP_MS
                    }
                    None => true,
                };
                if new {
                    sessions.push(Session {
                        person: e.person,
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
                if e.kind() == PURCHASE {
                    s.revenue += e.price() as i64;
                    s.has_purchase = true;
                }
            }
            sessions.shrink_to_fit();
            Part { events, sessions }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_size() {
        assert_eq!(std::mem::size_of::<Session>(), 40);
        assert_eq!(std::mem::size_of::<Event>(), 32);
    }

    #[test]
    fn persons_never_merge_and_stay_in_ts_order() {
        // two persons interleaved in time, plus one other person
        let (a, b, c) = (Key::inline(b"a"), Key::inline(b"b"), Key::inline(b"c"));
        let ev = |p: Key, ts: i64| Event::test(p, ts, 0, 0);
        let v = vec![ev(b, 30), ev(a, 10), ev(c, 5), ev(b, 20), ev(a, 40), ev(a, 25)];
        let p = &sessionize_part(v);
        let seq: Vec<(Key, i64)> = p.events.iter().map(|e| (e.person, e.ts)).collect();
        assert_eq!(seq, vec![(a, 10), (a, 25), (a, 40), (b, 20), (b, 30), (c, 5)]);
        // gaps are ms here, far under 30 minutes: one session per person, never one across persons
        let s: Vec<(Key, u32)> = p.sessions.iter().map(|s| (s.person, s.n_events)).collect();
        assert_eq!(s, vec![(a, 3), (b, 2), (c, 1)]);
    }

    #[test]
    fn gap_rule() {
        let a = Key::inline(b"a");
        let ev = |ts: i64| Event::test(a, ts, 0, 0);
        let v = vec![ev(0), ev(GAP_MS), ev(2 * GAP_MS + 1), ev(2 * GAP_MS + 1)];
        let p = sessionize_part(v);
        let s: Vec<(i64, u32)> = p.sessions.iter().map(|s| (s.start, s.n_events)).collect();
        // a gap of exactly 30 minutes stays in the session; equal timestamps always share one
        assert_eq!(s, vec![(0, 2), (2 * GAP_MS + 1, 2)]);
    }
}
