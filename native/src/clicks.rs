//! Stage 6: clicks dedup, validation, person, credit and assists (rules.md "Clicks").
//! Mirrors `pipeline/clicks.py`.
use std::cmp::Ordering;

use rayon::prelude::*;

use crate::raw::Row;
use crate::sessions::Part;
use crate::ts::parse_ts;
use crate::util::{cmp_nulls_last, hash_bytes, partition, prof, FxMap};

pub const WEEK_MS: i64 = 604_800_000;
pub const ORGANIC: &[u8] = b"organic";

/// Campaigns interned as u32 ids. `organic` is the id of the key "organic" (the oracle keys credit by the
/// campaign string, so a real campaign named "organic" would merge with organic there too).
pub struct Campaigns<'a> {
    pub names: Vec<&'a [u8]>,
    pub channels: Vec<Option<&'a [u8]>>,
    pub idx: FxMap<&'a [u8], u32>,
    pub organic: u32,
}

impl<'a> Campaigns<'a> {
    pub fn new(rows: &[Row<'a, 2>]) -> Campaigns<'a> {
        let mut c = Campaigns { names: Vec::new(), channels: Vec::new(), idx: FxMap::default(), organic: 0 };
        for r in rows {
            if let Some(id) = r[0] {
                if !c.idx.contains_key(id) {
                    c.idx.insert(id, c.names.len() as u32);
                    c.names.push(id);
                    c.channels.push(r[1]);
                }
            }
        }
        c.organic = match c.idx.get(ORGANIC) {
            Some(&i) => i,
            None => {
                c.names.push(ORGANIC);
                c.channels.push(None);
                (c.names.len() - 1) as u32
            }
        };
        c.channels[c.organic as usize] = Some(ORGANIC);
        c
    }
}

#[derive(Clone, Copy)]
pub struct Click<'a> {
    pub person: &'a [u8],
    pub click_id: &'a [u8],
    pub camp: u32,
    pub ts: i64,
}

/// Per session part: the credited campaign of every session (the organic id when none) and the assists.
pub struct Credit {
    pub credit: Vec<u32>,
    /// (session index in the part, campaign), distinct per session; only filled when `keep_assists`
    pub assists: Vec<(u32, u32)>,
    /// assisted sessions per key(campaign, day)
    pub assist_counts: FxMap<u64, i64>,
}

#[inline]
pub fn key(camp: u32, day: i32) -> u64 {
    ((camp as u64) << 32) | day as u32 as u64
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

/// Dedup on click_id, validate (user present, campaign known, ts in [ws - 7d, we)), map to the person.
pub fn valid_clicks<'a>(
    rows: &[Row<'a, 4>],
    pmap: &FxMap<&'a [u8], &'a [u8]>,
    camps: &Campaigns<'a>,
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
                    let camp = *camps.idx.get(r[2]?)?;
                    let ts = parse_ts(r[3]?)?;
                    if ts < ws - WEEK_MS || ts >= we {
                        return None;
                    }
                    let person = pmap.get(user).copied().unwrap_or(user);
                    Some(Click { person, click_id: r[0].unwrap(), camp, ts })
                })
                .collect()
        })
        .collect();
    kept.into_iter().flatten().collect()
}

/// Credit and assists for every session, part by part (clicks are split with the sessions' partitioning).
pub fn credit<'a>(parts: &[Part<'a>], clicks: &[Click<'a>], camps: &Campaigns<'a>, keep_assists: bool) -> Vec<Credit> {
    let mut t = std::time::Instant::now();
    let cparts = partition(clicks.len(), |i| Some(hash_bytes(clicks[i].person)));
    assert_eq!(cparts.len(), parts.len());
    prof("credit: partition clicks by person", &mut t);
    let ncamp = camps.names.len();
    let r = parts
        .par_iter()
        .zip(cparts.par_iter())
        .map(|(p, cidx)| {
            let mut ck: Vec<Click<'a>> = cidx.iter().map(|&i| clicks[i as usize]).collect();
            ck.sort_unstable_by(|a, b| a.person.cmp(b.person).then(a.ts.cmp(&b.ts)).then(a.click_id.cmp(b.click_id)));
            // person -> range in ck
            let mut ranges: FxMap<&'a [u8], (u32, u32)> =
                FxMap::with_capacity_and_hasher(ck.len() / 2, Default::default());
            let mut i = 0;
            while i < ck.len() {
                let mut j = i + 1;
                while j < ck.len() && ck[j].person == ck[i].person {
                    j += 1;
                }
                ranges.insert(ck[i].person, (i as u32, j as u32));
                i = j;
            }
            let mut out = Credit {
                credit: Vec::with_capacity(p.sessions.len()),
                assists: Vec::new(),
                assist_counts: FxMap::default(),
            };
            // stamp[c] == session number + 1 when campaign c already assists the current session
            let mut stamp = vec![0u32; ncamp];
            let mut last_person: &[u8] = &[];
            let mut pc: &[Click<'a>] = &[];
            for (si, s) in p.sessions.iter().enumerate() {
                if s.person != last_person {
                    last_person = s.person;
                    pc = match ranges.get(s.person) {
                        Some(&(a, b)) => &ck[a as usize..b as usize],
                        None => &[],
                    };
                }
                // clicks with ts <= start: pc[..k]
                let k = pc.partition_point(|c| c.ts <= s.start);
                let mut credit = camps.organic;
                if k > 0 {
                    let t = pc[k - 1].ts;
                    if s.start - t <= WEEK_MS {
                        // the first click at that ms has the smallest click_id
                        let f = pc[..k].partition_point(|c| c.ts < t);
                        credit = pc[f].camp;
                    }
                }
                out.credit.push(credit);
                let lo = pc[..k].partition_point(|c| c.ts < s.start - WEEK_MS);
                let tag = si as u32 + 1;
                for c in &pc[lo..k] {
                    if stamp[c.camp as usize] != tag {
                        stamp[c.camp as usize] = tag;
                        *out.assist_counts.entry(key(c.camp, s.day)).or_insert(0) += 1;
                        if keep_assists {
                            out.assists.push((si as u32, c.camp));
                        }
                    }
                }
            }
            out
        })
        .collect();
    prof("credit: sort clicks, credit, assists", &mut t);
    r
}
