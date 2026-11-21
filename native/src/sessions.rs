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

/// One hash partition by person: its sessions in order, and (only for the stage dumps) its events grouped
/// by person and sorted by ts.
pub struct Part {
    pub events: Vec<Event>,
    pub sessions: Vec<Session>,
}

pub fn day_of(ms: i64) -> i32 {
    ms.div_euclid(DAY_MS) as i32
}

/// The sort key of an event: (person key, ts) packed so that u128 order is exactly (person, ts) order.
#[inline(always)]
fn person_ts(e: &Event) -> u128 {
    ((e.person.raw() as u128) << 64) | ((e.ts as u64) ^ (1 << 63)) as u128
}

/// Every person bucket taken out of `by_person` (freed as it goes) into a reused buffer, sorted, cut into
/// sessions, and its user_daily rows built while it is hot; parts and rows in bucket order. A part keeps its
/// events only when `keep_events` (the stage dumps); nothing later needs them.
pub fn sessionize<'a>(
    by_person: &Chunks<Event>,
    cats: &crate::events::Categories<'a>,
    keep_events: bool,
) -> (Vec<Part>, Vec<Vec<crate::daily::UserDay<'a>>>) {
    (0..NPARTS)
        .into_par_iter()
        .map_init(Vec::new, |buf: &mut Vec<Event>, b| {
            buf.clear();
            by_person.take(b, buf);
            let sessions = sessions_of(buf);
            let ud = crate::daily::user_daily_part(&sessions, buf, cats);
            let events = if keep_events { buf.clone() } else { Vec::new() };
            (Part { events, sessions }, ud)
        })
        .unzip()
}

/// One person bucket's events, sorted and cut into sessions (tests).
pub fn sessionize_part(mut events: Vec<Event>) -> Part {
    let sessions = sessions_of(&mut events);
    Part { events, sessions }
}

/// Sort `events` by (person, ts) and cut them into sessions.
fn sessions_of(events: &mut [Event]) -> Vec<Session> {
    events.sort_unstable_by_key(person_ts);
    let mut sessions: Vec<Session> = Vec::with_capacity(events.len() / 4);
    for (i, e) in events.iter().enumerate() {
        let new = match sessions.last() {
            Some(s) => s.person != e.person || e.ts - events[i - 1].ts > GAP_MS,
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
    sessions
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
    fn packed_key_orders_as_person_then_ts() {
        let keys = [Key::inline(b""), Key::inline(b"0"), Key::inline(b"a"), Key::inline(b"zzzzzzzzzz")];
        let tss = [i64::MIN, -1, 0, 1, 1_753_000_000_000, i64::MAX];
        let mut all = Vec::new();
        for &k in &keys {
            for &t in &tss {
                all.push(Event::test(k, t, 0, 0));
            }
        }
        for a in &all {
            for b in &all {
                assert_eq!(person_ts(a).cmp(&person_ts(b)), (a.person, a.ts).cmp(&(b.person, b.ts)));
            }
        }
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
