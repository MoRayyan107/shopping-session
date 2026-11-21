//! Stage 6: clicks dedup, validation, person, credit and assists (rules.md "Clicks").
//! Mirrors `pipeline/clicks.py`.
use std::cmp::Ordering;

use rayon::prelude::*;

use crate::csv::{for_each_row, split_line};
use crate::key::Key;
use crate::links::PersonTable;
use crate::raw::{pieces_with_file, Mapped, Row};
use crate::sessions::Part;
use crate::span::{Span, Store};
use crate::ts::parse_ts;
use crate::util::{cmp_nulls_last, dedup_groups, dedup_order, map_in_order, sort_by_bucket, Buckets, FxMap, PART_BITS};

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

/// One valid click, mapped to its person (32 bytes).
#[derive(Clone, Copy)]
pub struct Click {
    /// the person's exact key; its bucket is the top bits of `person.hash()`
    pub person: Key,
    /// the click_id's exact key (inline keys order as the bytes do; see `cmp_click_ids`)
    pub click_id: Key,
    pub ts: i64,
    pub camp: u32,
}

/// Byte-wise order of two click_ids by key: inline keys order exactly as their bytes; a long key is read back.
#[inline]
fn cmp_click_ids(store: &Store<'_>, a: Key, b: Key) -> Ordering {
    if a.is_inline() && b.is_inline() {
        a.cmp(&b)
    } else {
        a.to_vec(store).cmp(&b.to_vec(store))
    }
}

/// Per session part: the credited campaign of every session (the organic id when none) and the assists.
pub struct Credit {
    pub credit: Vec<u32>,
    /// (session index in the part, campaign), distinct per session; only filled when `keep_assists`
    pub assists: Vec<(u32, u32)>,
    /// assisted sessions per key(campaign, day)
    pub assist_counts: FxMap<u64, i64>,
}

const VALID: u32 = 1 << 31;

/// One well-formed line with a non-empty click_id, validated (40 bytes).
#[derive(Clone, Copy)]
pub struct Rec {
    /// the click_id's exact key (its hash picks the dedup bucket)
    id: Key,
    /// the user ID's exact key (the default key when the copy is invalid)
    user: Key,
    line: Span,
    ts: i64,
    /// campaign id in the low 31 bits, VALID in the top bit
    camp_valid: u32,
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
pub fn build<'a>(
    files: &'a Mapped,
    first: u32,
    store: &Store<'a>,
    ptab: &PersonTable,
    camps: &Campaigns<'_>,
    ws: i64,
    we: i64,
) -> Vec<Vec<Click>> {
    let nparts = 1usize << PART_BITS;
    let bucket = |h: u64| (h >> (64 - PART_BITS)) as usize;
    assert!(camps.names.len() < VALID as usize, "too many campaigns");
    // pass 1: split + validate every line; each piece's records ordered by click_id-hash bucket
    let blocks: Vec<(Vec<Rec>, Vec<u32>)> = map_in_order(&pieces_with_file(files, first), Vec::new, |recs: &mut Vec<Rec>, &(file, piece)| {
            crate::raw::note_taken(piece.len());
            recs.clear();
            for_each_row::<4, _>(piece, |line, f| {
                let Some(id) = f[0] else { return };
                let id = store.key(file, id);
                let valid = || -> Option<(&'a [u8], u32, i64)> {
                    let user = f[1]?;
                    let camp = *camps.idx.get(f[2]?)?;
                    let ts = parse_ts(f[3]?)?;
                    if ts < ws - WEEK_MS || ts >= we {
                        return None;
                    }
                    Some((user, camp, ts))
                };
                let line_sp = store.span(file, line);
                let rec = match valid() {
                    Some((user, camp, ts)) => Rec { id, user: store.key(file, user), line: line_sp, ts, camp_valid: camp | VALID },
                    None => Rec { id, user: Key::default(), line: line_sp, ts: 0, camp_valid: 0 },
                };
                recs.push(rec);
            });
            sort_by_bucket(recs, |r| bucket(r.id.hash()))
        });
    let by_id = Buckets::new(blocks);
    // pass 2: dedup each id bucket; kept valid clicks go to person buckets
    let out: Vec<(Vec<Click>, Vec<u32>)> = (0..nparts)
        .into_par_iter()
        .map_init(
            || (Vec::new(), Vec::new()),
            |(recs, clicks): &mut (Vec<Rec>, Vec<Click>), b| {
                recs.clear();
                clicks.clear();
                by_id.gather(b, recs);
                let order = dedup_order(recs.len(), |i| recs[i].id.hash());
                let emit = |r: &Rec, clicks: &mut Vec<Click>| {
                    if r.camp_valid & VALID != 0 {
                        let person = ptab.lookup(r.user);
                        clicks.push(Click { person, click_id: r.id, camp: r.camp_valid & !VALID, ts: r.ts });
                    }
                };
                let (alone, groups) = dedup_groups(&order);
                // unique ids in memory order (sequential reads), then the groups of copies
                for (i, r) in recs.iter().enumerate() {
                    if alone[i] {
                        emit(r, clicks);
                    }
                }
                for grp in &groups {
                    // a truncated-hash group: runs of one click_id (equal keys <=> equal IDs)
                    let mut g: Vec<usize> = grp.clone();
                    g.sort_unstable_by_key(|&k| (recs[k].id, k));
                    let mut a = 0;
                    while a < g.len() {
                        let mut z = a + 1;
                        while z < g.len() && recs[g[z]].id == recs[g[a]].id {
                            z += 1;
                        }
                        let run = &g[a..z];
                        a = z;
                        // copies are identical values (rules.md), so any one will do when their parsed values
                        // agree; otherwise the oracle's order from the lines themselves
                        let r0 = &recs[run[0]];
                        let same = run.iter().all(|&k| {
                            let r = &recs[k];
                            (r.user, r.ts, r.camp_valid) == (r0.user, r0.ts, r0.camp_valid)
                        });
                        if same {
                            emit(r0, clicks);
                            continue;
                        }
                        let first = run
                            .iter()
                            .map(|&k| {
                                let mut f = [None; 4];
                                split_line(store.get(recs[k].line), &mut f);
                                (f, k)
                            })
                            .min_by(|x, y| copy_order(&x.0, &y.0).then(x.1.cmp(&y.1)))
                            .unwrap();
                        emit(&recs[first.1], clicks);
                    }
                }
                sort_by_bucket(clicks, |c| bucket(c.person.hash()))
            },
        )
        .collect();
    drop(by_id);
    let mut by_person = Buckets::new(out).into_vecs();
    by_person
        .par_iter_mut()
        .for_each(|v| {
            v.sort_unstable_by(|a, b| {
                a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)).then_with(|| cmp_click_ids(store, a.click_id, b.click_id))
            })
        });
    by_person
}

/// Credit and assists for every session, part by part. Sessions and clicks of a part are both ordered by
/// person key, so one forward walk finds each person's clicks.
pub fn credit(parts: &[Part], clicks: &[Vec<Click>], camps: &Campaigns<'_>, keep_assists: bool) -> Vec<Credit> {
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
            let mut pc: &[Click] = &[];
            let mut cur: Option<Key> = None;
            for (si, s) in p.sessions.iter().enumerate() {
                let sp = s.person;
                if cur != Some(sp) {
                    cur = Some(sp);
                    while j < ck.len() && ck[j].person < sp {
                        j += 1;
                    }
                    let mut e = j;
                    while e < ck.len() && ck[e].person == sp {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn record_sizes() {
        assert_eq!(std::mem::size_of::<Rec>(), 40);
        assert_eq!(std::mem::size_of::<Click>(), 32);
    }
    #[test]
    fn click_id_order_is_byte_order() {
        let d = b"k10,k9,k1,aaaaaaaaaaaaaaaaaaaa1,aaaaaaaaaaaaaaaaaaaa0,k".to_vec();
        let store = Store::from_slices(vec![&d], crate::span::MAX_LEN);
        let f: Vec<&[u8]> = d.split(|&b| b == b',').collect();
        for x in &f {
            for y in &f {
                assert_eq!(cmp_click_ids(&store, store.key(0, x), store.key(0, y)), x.cmp(y), "{x:?} {y:?}");
            }
        }
    }
}
