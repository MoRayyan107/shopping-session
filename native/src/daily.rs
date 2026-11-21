//! Stages 5 and 7: the output tables (rules.md "Outputs"). Mirrors `pipeline/outputs.py`.
use rayon::prelude::*;

use crate::clicks::{key, Campaigns, Cells, Counts, Credit};
use crate::events::{Categories, PURCHASE};
use crate::pq::{write_table, Col, USER_DAILY};
use crate::key::Key;
use crate::sessions::Part;
use crate::span::Store;
use crate::util::FxMap;

pub struct UserDay<'a> {
    pub user: Key,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top: Option<&'a [u8]>,
}

/// One row per (person, day of session start).
pub fn user_daily<'a>(parts: &[Part], cats: &Categories<'a>) -> Vec<Vec<UserDay<'a>>> {
    parts
        .par_iter()
        .map(|p| {
            let mut out: Vec<UserDay<'a>> = Vec::new();
            let mut amounts: Vec<(u32, i64)> = Vec::new();
            let ss = &p.sessions;
            let mut i = 0;
            while i < ss.len() {
                let (person, day) = (ss[i].person, ss[i].day);
                let mut row = UserDay { user: person, day, sessions: 0, events: 0, revenue: 0, top: None };
                amounts.clear();
                while i < ss.len() && ss[i].day == day && ss[i].person == person {
                    let s = &ss[i];
                    row.sessions += 1;
                    row.events += s.n_events as i64;
                    row.revenue += s.revenue;
                    if s.has_purchase {
                        for e in &p.events[s.first as usize..(s.first + s.n_events) as usize] {
                            if e.kind() == PURCHASE {
                                match amounts.iter_mut().find(|c| c.0 == e.cat) {
                                    Some(c) => c.1 += e.price() as i64,
                                    None => amounts.push((e.cat, e.price() as i64)),
                                }
                            }
                        }
                    }
                    i += 1;
                }
                // highest amount; a tie goes to the byte-wise smallest category
                row.top = amounts
                    .iter()
                    .map(|&(c, a)| (cats.names[c as usize], a))
                    .max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(a.0)))
                    .map(|c| c.0);
                out.push(row);
            }
            out
        })
        .collect()
}

pub fn user_daily_cols(store: &Store<'_>, rows: &[Vec<UserDay<'_>>]) -> Vec<Col> {
    let all = || rows.iter().flatten();
    vec![
        Col::Str(all().map(|r| r.user.to_vec(store)).collect()),
        Col::Date(all().map(|r| r.day).collect()),
        Col::I64(all().map(|r| r.sessions).collect()),
        Col::I64(all().map(|r| r.events).collect()),
        Col::I64(all().map(|r| r.revenue).collect()),
        Col::OptStr(all().map(|r| r.top.map(<[u8]>::to_vec)).collect()),
    ]
}

pub struct CampaignDay<'a> {
    pub campaign: &'a [u8],
    pub channel: Option<&'a [u8]>,
    pub day: i32,
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assisted: i64,
}

/// One row per (campaign, day) that was credited or assisted; organic is the campaign "organic".
pub fn campaign_daily<'a>(parts: &[Part], credit: &[Credit], camps: &Campaigns<'a>, cells: &Cells) -> Vec<CampaignDay<'a>> {
    if cells.dense {
        return campaign_daily_dense(parts, credit, camps, cells);
    }
    let partial: Vec<FxMap<u64, [i64; 4]>> = parts
        .par_iter()
        .zip(credit.par_iter())
        .map(|(p, c)| {
            let mut m: FxMap<u64, [i64; 4]> = FxMap::default();
            for (s, &k) in p.sessions.iter().zip(&c.credit) {
                let v = m.entry(key(k, s.day)).or_insert([0; 4]);
                v[0] += 1;
                v[1] += s.has_purchase as i64;
                v[2] += s.revenue;
            }
            let Counts::Map(counts) = &c.assist_counts else { unreachable!("map cells give map counts") };
            for (k, n) in counts {
                m.entry(*k).or_insert([0; 4])[3] += n;
            }
            m
        })
        .collect();
    let mut all: FxMap<u64, [i64; 4]> = FxMap::default();
    for m in partial {
        for (k, v) in m {
            let t = all.entry(k).or_insert([0; 4]);
            for i in 0..4 {
                t[i] += v[i];
            }
        }
    }
    all.into_iter()
        .map(|(k, v)| {
            let camp = (k >> 32) as usize;
            CampaignDay {
                campaign: camps.names[camp],
                channel: camps.channels[camp],
                day: k as u32 as i32,
                sessions: v[0],
                purchasing: v[1],
                revenue: v[2],
                assisted: v[3],
            }
        })
        .collect()
}

/// campaign_daily with dense (campaign, day) cells: each thread sums its parts into one array (at most 8
/// arrays: parts are taken 32 at a time), the arrays are added, and every cell with a credited or assisted
/// session is a row (exactly the keys the map version would hold).
fn campaign_daily_dense<'a>(parts: &[Part], credit: &[Credit], camps: &Campaigns<'a>, cells: &Cells) -> Vec<CampaignDay<'a>> {
    let n = cells.len();
    let tot: Vec<[i64; 4]> = parts
        .par_iter()
        .zip(credit.par_iter())
        .with_min_len(32)
        .fold(
            || vec![[0i64; 4]; n],
            |mut acc, (p, c)| {
                for (s, &k) in p.sessions.iter().zip(&c.credit) {
                    let v = &mut acc[cells.cell(k, s.day)];
                    v[0] += 1;
                    v[1] += s.has_purchase as i64;
                    v[2] += s.revenue;
                }
                let Counts::Dense(counts) = &c.assist_counts else { unreachable!("dense cells give dense counts") };
                for (v, &a) in acc.iter_mut().zip(counts) {
                    v[3] += a as i64;
                }
                acc
            },
        )
        .reduce(
            || vec![[0i64; 4]; n],
            |mut a, b| {
                for (x, y) in a.iter_mut().zip(&b) {
                    for i in 0..4 {
                        x[i] += y[i];
                    }
                }
                a
            },
        );
    tot.iter()
        .enumerate()
        .filter(|(_, v)| v[0] > 0 || v[3] > 0)
        .map(|(cell, v)| {
            let (camp, day) = cells.of(cell);
            CampaignDay {
                campaign: camps.names[camp as usize],
                channel: camps.channels[camp as usize],
                day,
                sessions: v[0],
                purchasing: v[1],
                revenue: v[2],
                assisted: v[3],
            }
        })
        .collect()
}

pub fn campaign_daily_cols(rows: &[CampaignDay<'_>]) -> Vec<Col> {
    vec![
        Col::Str(rows.iter().map(|r| r.campaign.to_vec()).collect()),
        Col::OptStr(rows.iter().map(|r| r.channel.map(<[u8]>::to_vec)).collect()),
        Col::Date(rows.iter().map(|r| r.day).collect()),
        Col::I64(rows.iter().map(|r| r.sessions).collect()),
        Col::I64(rows.iter().map(|r| r.purchasing).collect()),
        Col::I64(rows.iter().map(|r| r.revenue).collect()),
        Col::I64(rows.iter().map(|r| r.assisted).collect()),
    ]
}

/// user_daily as `nfiles` part files written in parallel (rules.md: file count does not matter); every file
/// is written, even an empty one, so the folder always holds at least one.
pub fn write_user_daily(store: &Store<'_>, dir: &std::path::Path, rows: &[Vec<UserDay<'_>>], nfiles: usize) -> parquet::errors::Result<()> {
    let nfiles = nfiles.clamp(1, rows.len().max(1));
    let per = rows.len().div_ceil(nfiles).max(1);
    (0..nfiles).into_par_iter().try_for_each(|f| {
        let group = &rows[(f * per).min(rows.len())..((f + 1) * per).min(rows.len())];
        write_table(&dir.join(format!("part-{f}.parquet")), USER_DAILY, user_daily_cols(store, group))
    })
}
