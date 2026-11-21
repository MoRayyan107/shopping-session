//! Stage 4: sessions and day (rules.md "Events, in this order", steps 6-7).
//! Mirrors `pipeline/events.py` from `sess_ts` to `sessions`.
use std::cmp::Ordering;

use rayon::prelude::*;

use crate::events::{Event, PURCHASE};
use crate::span::{Span, Store};

pub const GAP_MS: i64 = 1_800_000;
pub const DAY_MS: i64 = 86_400_000;

/// One session (48 bytes).
#[derive(Clone, Copy)]
pub struct Session {
    pub person: Span,
    pub phash: u64,
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

/// Same person: equal spans are equal bytes, so the bytes are only read when the spans differ.
#[inline(always)]
pub fn same_person(store: &Store<'_>, (ha, pa): (u64, Span), (hb, pb): (u64, Span)) -> bool {
    ha == hb && (pa == pb || store.get(pa) == store.get(pb))
}

/// Order of persons as stored: by hash, then by bytes.
#[inline(always)]
pub fn cmp_person(store: &Store<'_>, (ha, pa): (u64, Span), (hb, pb): (u64, Span)) -> Ordering {
    ha.cmp(&hb).then_with(|| if pa == pb { Ordering::Equal } else { store.get(pa).cmp(store.get(pb)) })
}

/// Sort a bucket so each person's events are contiguous and in ts order. Sorting is by (hash, ts, tie); if
/// two different persons share a 64-bit hash (checked), the bucket is sorted again by (hash, person, ts, tie).
/// `key` gives (person hash, person, ts, tie-break bytes).
pub fn group_by_person<'a, T, F>(store: &Store<'a>, v: &mut [T], key: F)
where
    F: Fn(&T) -> (u64, Span, i64, &'a [u8]) + Copy,
{
    v.sort_unstable_by(|a, b| {
        let (ha, _, ta, xa) = key(a);
        let (hb, _, tb, xb) = key(b);
        ha.cmp(&hb).then(ta.cmp(&tb)).then_with(|| xa.cmp(xb))
    });
    let collision = v.windows(2).any(|w| {
        let (ha, pa, _, _) = key(&w[0]);
        let (hb, pb, _, _) = key(&w[1]);
        ha == hb && !same_person(store, (ha, pa), (hb, pb))
    });
    if collision {
        v.sort_unstable_by(|a, b| {
            let (ha, pa, ta, xa) = key(a);
            let (hb, pb, tb, xb) = key(b);
            cmp_person(store, (ha, pa), (hb, pb)).then(ta.cmp(&tb)).then_with(|| xa.cmp(xb))
        });
    }
}

pub fn sessionize(store: &Store<'_>, by_person: Vec<Vec<Event>>) -> Vec<Part> {
    by_person
        .into_par_iter()
        .map(|mut events| {
            group_by_person(store, &mut events, |e| (e.phash, e.person, e.ts, &[]));
            let mut sessions: Vec<Session> = Vec::with_capacity(events.len() / 4);
            for (i, e) in events.iter().enumerate() {
                let new = match sessions.last() {
                    Some(s) => {
                        !same_person(store, (s.phash, s.person), (e.phash, e.person)) || e.ts - events[i - 1].ts > GAP_MS
                    }
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
                if e.kind() == PURCHASE {
                    s.revenue += e.price() as i64;
                    s.has_purchase = true;
                }
            }
            sessions.shrink_to_fit();
            Part { events, sessions }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_size() {
        assert_eq!(std::mem::size_of::<Session>(), 48);
    }

    #[test]
    fn colliding_person_hashes_never_merge() {
        let d = b"abc".to_vec();
        let store = Store::from_slices(vec![&d], crate::span::MAX_LEN);
        let (a, b, c) = (store.span(0, &d[0..1]), store.span(0, &d[1..2]), store.span(0, &d[2..3]));
        let ev = |p: Span, h: u64, ts: i64| Event::test(p, h, ts, 0, 0);
        // two persons with the same (forced) hash, interleaved in time, plus one other person
        let v = vec![ev(b, 7, 30), ev(a, 7, 10), ev(c, 1, 5), ev(b, 7, 20), ev(a, 7, 40), ev(a, 7, 25)];
        let parts = sessionize(&store, vec![v]);
        let p = &parts[0];
        // each person's events contiguous and in ts order
        let seq: Vec<(&[u8], i64)> = p.events.iter().map(|e| (store.get(e.person), e.ts)).collect();
        assert_eq!(seq, vec![(&b"c"[..], 5), (b"a", 10), (b"a", 25), (b"a", 40), (b"b", 20), (b"b", 30)]);
        // gaps are ms here, far under 30 minutes: one session per person, never one across persons
        let s: Vec<(&[u8], u32)> = p.sessions.iter().map(|s| (store.get(s.person), s.n_events)).collect();
        assert_eq!(s, vec![(&b"c"[..], 1), (b"a", 3), (b"b", 2)]);
    }

    #[test]
    fn equal_bytes_with_different_spans_are_one_person() {
        // the same ID read from two places: different spans, one person, one session
        let d = b"u1,u1".to_vec();
        let store = Store::from_slices(vec![&d], crate::span::MAX_LEN);
        let (x, y) = (store.span(0, &d[0..2]), store.span(0, &d[3..5]));
        assert_ne!(x, y);
        let v = vec![Event::test(x, 9, 10, 0, 0), Event::test(y, 9, 20, 0, 0)];
        let parts = sessionize(&store, vec![v]);
        assert_eq!(parts[0].sessions.len(), 1);
        assert_eq!(parts[0].sessions[0].n_events, 2);
    }

    #[test]
    fn gap_rule() {
        let d = b"a".to_vec();
        let store = Store::from_slices(vec![&d], crate::span::MAX_LEN);
        let a = store.span(0, &d[..]);
        let ev = |ts: i64| Event::test(a, 1, ts, 0, 0);
        let v = vec![ev(0), ev(GAP_MS), ev(2 * GAP_MS + 1), ev(2 * GAP_MS + 1)];
        let parts = sessionize(&store, vec![v]);
        let s: Vec<(i64, u32)> = parts[0].sessions.iter().map(|s| (s.start, s.n_events)).collect();
        // a gap of exactly 30 minutes stays in the session; equal timestamps always share one
        assert_eq!(s, vec![(0, 2), (2 * GAP_MS + 1, 2)]);
    }
}
