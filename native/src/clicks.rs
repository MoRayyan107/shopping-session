//! Stage 6: clicks dedup, validation, person, credit and assists (rules.md "Clicks").
//! Mirrors `pipeline/clicks.py`.
use std::cmp::Ordering;

use rayon::prelude::*;

use crate::csv::{lines, split_line};
use crate::links::PersonTable;
use crate::raw::{pieces, Mapped, Row};
use crate::sessions::{group_by_person, Part};
use crate::ts::parse_ts;
use crate::util::{cmp_nulls_last, hash_bytes, transpose_concat, FxMap, PART_BITS};

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
    pub phash: u64,
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

/// One well-formed line with a non-empty click_id, validated.
#[derive(Clone, Copy)]
struct Rec<'a> {
    idhash: u64,
    uhash: u64,
    id: &'a [u8],
    user: &'a [u8],
    line: &'a [u8],
    ts: i64,
    camp: u32,
    valid: bool,
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
/// Validation runs in the parse pass (it depends only on the copy); copies are resolved like the oracle.
/// Returned bucketed by hash(person) exactly like the events (same hash, same bits), each bucket grouped
/// by person and sorted by (ts, click_id).
pub fn build<'a>(files: &'a Mapped, ptab: &PersonTable<'a>, camps: &Campaigns<'a>, ws: i64, we: i64) -> Vec<Vec<Click<'a>>> {
    let nparts = 1usize << PART_BITS;
    let bucket = |h: u64| (h >> (64 - PART_BITS)) as usize;
    // pass 1: split + validate every line into click_id-hash buckets
    let blocks: Vec<Vec<Vec<Rec<'a>>>> = pieces(files)
        .par_iter()
        .map(|piece| {
            let mut out: Vec<Vec<Rec<'a>>> = (0..nparts).map(|_| Vec::with_capacity(piece.len() / 60 / nparts * 2)).collect();
            let mut f: Row<'a, 4> = [None; 4];
            for line in lines(piece) {
                if !split_line(line, &mut f) {
                    continue;
                }
                let Some(id) = f[0] else { continue };
                let idhash = hash_bytes(id);
                let valid = || -> Option<(&'a [u8], u32, i64)> {
                    let user = f[1]?;
                    let camp = *camps.idx.get(f[2]?)?;
                    let ts = parse_ts(f[3]?)?;
                    if ts < ws - WEEK_MS || ts >= we {
                        return None;
                    }
                    Some((user, camp, ts))
                };
                let rec = match valid() {
                    Some((user, camp, ts)) => Rec { idhash, uhash: hash_bytes(user), id, user, line, ts, camp, valid: true },
                    None => Rec { idhash, uhash: 0, id, user: &[], line, ts: 0, camp: 0, valid: false },
                };
                out[bucket(idhash)].push(rec);
            }
            out
        })
        .collect();
    let by_id = transpose_concat(blocks);
    // pass 2: dedup each id bucket; kept valid clicks go to person buckets
    let out: Vec<Vec<Vec<Click<'a>>>> = by_id
        .into_par_iter()
        .map(|mut recs| {
            recs.sort_unstable_by_key(|r| r.idhash);
            let mut buckets: Vec<Vec<Click<'a>>> =
                (0..nparts).map(|_| Vec::with_capacity(recs.len() / nparts + 16)).collect();
            let emit = |r: &Rec<'a>, buckets: &mut Vec<Vec<Click<'a>>>| {
                if r.valid {
                    let (person, phash) = ptab.lookup(r.uhash, r.user);
                    buckets[bucket(phash)].push(Click { person, phash, click_id: r.id, camp: r.camp, ts: r.ts });
                }
            };
            let mut i = 0;
            while i < recs.len() {
                let mut j = i + 1;
                while j < recs.len() && recs[j].idhash == recs[i].idhash {
                    j += 1;
                }
                if j == i + 1 {
                    emit(&recs[i], &mut buckets);
                } else {
                    // copies of one click_id (or a hash collision): order by (click_id, the oracle's copy order)
                    let mut g: Vec<(Row<'a, 4>, usize)> = (i..j)
                        .map(|k| {
                            let mut f = [None; 4];
                            split_line(recs[k].line, &mut f);
                            (f, k)
                        })
                        .collect();
                    g.sort_unstable_by(|a, b| a.0[0].cmp(&b.0[0]).then_with(|| copy_order(&a.0, &b.0)));
                    for (n, (row, k)) in g.iter().enumerate() {
                        if n == 0 || g[n - 1].0[0] != row[0] {
                            emit(&recs[*k], &mut buckets);
                        }
                    }
                }
                i = j;
            }
            buckets
        })
        .collect();
    let mut by_person = transpose_concat(out);
    by_person.par_iter_mut().for_each(|v| group_by_person(v, |c| (c.phash, c.person, c.ts, c.click_id)));
    by_person
}

/// Credit and assists for every session, part by part. Sessions and clicks of a part are both ordered by
/// (person hash, person), so one forward walk finds each person's clicks.
pub fn credit(parts: &[Part<'_>], clicks: &[Vec<Click<'_>>], camps: &Campaigns<'_>, keep_assists: bool) -> Vec<Credit> {
    assert_eq!(clicks.len(), parts.len());
    let ncamp = camps.names.len();
    parts
        .par_iter()
        .zip(clicks.par_iter())
        .map(|(p, ck)| {
            let mut out = Credit {
                credit: Vec::with_capacity(p.sessions.len()),
                assists: Vec::new(),
                assist_counts: FxMap::default(),
            };
            // stamp[c] == session number + 1 when campaign c already assists the current session
            let mut stamp = vec![0u32; ncamp];
            let mut j = 0usize; // first click not yet passed
            let mut pc: &[Click<'_>] = &[];
            let mut cur: Option<(u64, &[u8])> = None;
            for (si, s) in p.sessions.iter().enumerate() {
                if cur != Some((s.phash, s.person)) {
                    cur = Some((s.phash, s.person));
                    while j < ck.len() && (ck[j].phash, ck[j].person) < (s.phash, s.person) {
                        j += 1;
                    }
                    let mut e = j;
                    while e < ck.len() && ck[e].phash == s.phash && ck[e].person == s.person {
                        e += 1;
                    }
                    pc = &ck[j..e];
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
        .collect()
}
