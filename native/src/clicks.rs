//! Stage 6: clicks dedup, validation, person, credit and assists (rules.md "Clicks").
//! Mirrors `pipeline/clicks.py`.
use std::cmp::Ordering;

use rayon::prelude::*;

use crate::raw::Row;
use crate::sessions::Part;
use crate::ts::parse_ts;
use crate::util::{cmp_nulls_last, hash_bytes, partition, FxMap};

pub const WEEK_MS: i64 = 604_800_000;

#[derive(Clone, Copy)]
pub struct Click<'a> {
    pub person: &'a [u8],
    pub click_id: &'a [u8],
    pub campaign: &'a [u8],
    pub ts: i64,
}

/// Per session part: the credited campaign of every session (None = organic) and the assists.
pub struct Credit<'a> {
    pub credit: Vec<Option<&'a [u8]>>,
    /// (session index in the part, campaign), distinct per session
    pub assists: Vec<(u32, &'a [u8])>,
}

fn copy_order(a: &Row<'_, 4>, b: &Row<'_, 4>) -> Ordering {
    for col in [1usize, 2, 3] {
        let o = cmp_nulls_last(a[col], b[col]);
        if o != Ordering::Equal {
            return o;
        }
    }
    Ordering::Equal
}

pub fn campaign_map<'a>(campaigns: &[Row<'a, 2>]) -> FxMap<&'a [u8], Option<&'a [u8]>> {
    let mut m = FxMap::with_capacity_and_hasher(campaigns.len(), Default::default());
    for c in campaigns {
        if let Some(id) = c[0] {
            m.entry(id).or_insert(c[1]);
        }
    }
    m
}

/// Dedup on click_id, validate (user present, campaign known, ts in [ws - 7d, we)), map to the person;
/// returned already hash-partitioned by person exactly like the sessions (same hash, same partitions).
pub fn valid_clicks<'a>(
    rows: &[Row<'a, 4>],
    pmap: &FxMap<&'a [u8], &'a [u8]>,
    camps: &FxMap<&'a [u8], Option<&'a [u8]>>,
    ws: i64,
    we: i64,
) -> Vec<Click<'a>> {
    let parts = partition(rows.len(), |i| rows[i][0].map(hash_bytes));
    let kept: Vec<Vec<Click<'a>>> = parts
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
            best.into_values()
                .filter_map(|i| {
                    let r = &rows[i as usize];
                    let user = r[1]?;
                    let campaign = r[2]?;
                    if !camps.contains_key(campaign) {
                        return None;
                    }
                    let ts = parse_ts(r[3]?)?;
                    if ts < ws - WEEK_MS || ts >= we {
                        return None;
                    }
                    let person = pmap.get(user).copied().unwrap_or(user);
                    Some(Click { person, click_id: r[0].unwrap(), campaign, ts })
                })
                .collect()
        })
        .collect();
    kept.into_iter().flatten().collect()
}

/// Credit and assists for every session, part by part (clicks are split with the sessions' partitioning).
pub fn credit<'a>(parts: &[Part<'a>], clicks: &[Click<'a>]) -> Vec<Credit<'a>> {
    let cparts = partition(clicks.len(), |i| Some(hash_bytes(clicks[i].person)));
    assert_eq!(cparts.len(), parts.len());
    parts
        .par_iter()
        .zip(cparts.par_iter())
        .map(|(p, cidx)| {
            let mut ck: Vec<Click<'a>> = cidx.iter().map(|&i| clicks[i as usize]).collect();
            ck.sort_unstable_by(|a, b| a.person.cmp(b.person).then(a.ts.cmp(&b.ts)).then(a.click_id.cmp(b.click_id)));
            // person -> range in ck
            let mut ranges: FxMap<&'a [u8], (u32, u32)> = FxMap::default();
            let mut i = 0;
            while i < ck.len() {
                let mut j = i + 1;
                while j < ck.len() && ck[j].person == ck[i].person {
                    j += 1;
                }
                ranges.insert(ck[i].person, (i as u32, j as u32));
                i = j;
            }
            let mut out = Credit { credit: Vec::with_capacity(p.sessions.len()), assists: Vec::new() };
            let mut seen: Vec<&'a [u8]> = Vec::new();
            for (si, s) in p.sessions.iter().enumerate() {
                let Some(&(a, b)) = ranges.get(s.person) else {
                    out.credit.push(None);
                    continue;
                };
                let pc = &ck[a as usize..b as usize];
                // clicks with ts <= start: pc[..k]
                let k = pc.partition_point(|c| c.ts <= s.start);
                let credit = if k == 0 {
                    None
                } else {
                    let t = pc[k - 1].ts;
                    if s.start - t > WEEK_MS {
                        None
                    } else {
                        // first click at that ts has the smallest click_id
                        let f = pc[..k].partition_point(|c| c.ts < t);
                        Some(pc[f].campaign)
                    }
                };
                out.credit.push(credit);
                let lo = pc[..k].partition_point(|c| c.ts < s.start - WEEK_MS);
                seen.clear();
                for c in &pc[lo..k] {
                    if !seen.contains(&c.campaign) {
                        seen.push(c.campaign);
                    }
                }
                out.assists.extend(seen.iter().map(|c| (si as u32, *c)));
            }
            out
        })
        .collect()
}
