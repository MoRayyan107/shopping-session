//! Stages 5 and 7: the output tables (rules.md "Outputs"). Mirrors `pipeline/outputs.py`.
use rayon::prelude::*;

use crate::events::PURCHASE;
use crate::pq::{write_table, Col, USER_DAILY};
use crate::sessions::Part;

pub struct UserDay<'a> {
    pub user: &'a [u8],
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top: Option<&'a [u8]>,
}

/// One row per (person, day of session start).
pub fn user_daily<'a>(parts: &[Part<'a>]) -> Vec<Vec<UserDay<'a>>> {
    parts
        .par_iter()
        .map(|p| {
            let mut out: Vec<UserDay<'a>> = Vec::new();
            let mut cats: Vec<(&'a [u8], i64)> = Vec::new();
            let ss = &p.sessions;
            let mut i = 0;
            while i < ss.len() {
                let (person, day) = (ss[i].person, ss[i].day);
                let mut row = UserDay { user: person, day, sessions: 0, events: 0, revenue: 0, top: None };
                cats.clear();
                while i < ss.len() && ss[i].person == person && ss[i].day == day {
                    let s = &ss[i];
                    row.sessions += 1;
                    row.events += s.n_events as i64;
                    row.revenue += s.revenue;
                    if s.has_purchase {
                        for e in &p.events[s.first as usize..(s.first + s.n_events) as usize] {
                            if e.ev.kind == PURCHASE {
                                match cats.iter_mut().find(|c| c.0 == e.category) {
                                    Some(c) => c.1 += e.ev.price,
                                    None => cats.push((e.category, e.ev.price)),
                                }
                            }
                        }
                    }
                    i += 1;
                }
                // highest amount; a tie goes to the byte-wise smallest category
                row.top = cats.iter().max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(a.0))).map(|c| c.0);
                out.push(row);
            }
            out
        })
        .collect()
}

pub fn user_daily_cols(rows: &[Vec<UserDay<'_>>]) -> Vec<Col> {
    let all = || rows.iter().flatten();
    vec![
        Col::Str(all().map(|r| r.user.to_vec()).collect()),
        Col::Date(all().map(|r| r.day).collect()),
        Col::I64(all().map(|r| r.sessions).collect()),
        Col::I64(all().map(|r| r.events).collect()),
        Col::I64(all().map(|r| r.revenue).collect()),
        Col::OptStr(all().map(|r| r.top.map(<[u8]>::to_vec)).collect()),
    ]
}

pub fn write_user_daily(path: &std::path::Path, rows: &[Vec<UserDay<'_>>]) -> parquet::errors::Result<()> {
    write_table(path, USER_DAILY, &user_daily_cols(rows))
}
