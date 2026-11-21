//! Stage 6: clicks dedup, validation, person, credit and assists (rules.md "Clicks").
//! Mirrors `pipeline/clicks.py`.
use std::cmp::Ordering;

use rayon::prelude::*;

use crate::csv::{lines, split_line};
use crate::key::Key;
use crate::links::PersonTable;
use crate::raw::{pieces_with_file, Mapped, Row};
use crate::sessions::{group_by_person, Part};
use crate::span::{Span, Store};
use crate::ts::parse_ts;
use crate::util::{cmp_nulls_last, dedup_groups, dedup_order, hash_bytes, map_in_order, sort_by_bucket, Buckets, FxMap, PART_BITS};

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

/// One valid click, mapped to its person (40 bytes).
#[derive(Clone, Copy)]
pub struct Click {
    /// the person's exact key; its bucket is the top bits of `person.hash()`
    pub person: Key,
    pub click_id: Span,
    pub ts: i64,
    pub camp: u32,
}

/// Per session part: the credited campaign of every session (the organic id when none) and the assists.
pub struct Credit {
    pub credit: Vec<u32>,
    /// (session index in the part, campaign), distinct per session; only filled when `keep_assists`
    pub assists: Vec<(u32, u32)>,
    /// assisted sessions per key(campaign, day)
    pub assist_counts: FxMap<u64, i64>,
}

/// click_id is field 0, so it starts the line (after a quote when quoted): `id_meta` holds its length and
/// a quoted flag; RESPLIT (a length that does not fit) means "split the line again".
const QUOTED: u32 = 1 << 31;
const RESPLIT: u32 = u32::MAX;
const VALID: u32 = 1 << 31;

/// One well-formed line with a non-empty click_id, validated (48 bytes).
#[derive(Clone, Copy)]
pub struct Rec {
    idhash: u64,
    /// the user ID's exact key (the default key when the copy is invalid)
    user: Key,
    line: Span,
    ts: i64,
    /// campaign id in the low 31 bits, VALID in the top bit
    camp_valid: u32,
    id_meta: u32,
}

impl Rec {
    #[inline]
    fn id<'a>(&self, store: &Store<'a>) -> &'a [u8] {
        let line = store.get(self.line);
        if self.id_meta == RESPLIT {
            let mut f = [None; 4];
            split_line(line, &mut f);
            return f[0].unwrap();
        }
        let len = (self.id_meta & !QUOTED) as usize;
        let at = (self.id_meta & QUOTED != 0) as usize;
        &line[at..at + len]
    }
}

#[inline]
fn id_meta(line: &[u8], id: &[u8]) -> u32 {
    let at = id.as_ptr() as usize - line.as_ptr() as usize;
    if id.len() < QUOTED as usize - 1 {
        id.len() as u32 | if at == 1 { QUOTED } else { 0 }
    } else {
        RESPLIT
    }
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
            let mut f: Row<'a, 4> = [None; 4];
            for line in lines(piece) {
                if !split_line(line, &mut f) {
                    continue;
                }
                let Some(id) = f[0] else { continue };
                let idhash = hash_bytes(id);
                let meta = id_meta(line, id);
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
                    Some((user, camp, ts)) => Rec {
                        idhash,
                        user: store.key(file, user),
                        line: line_sp,
                        ts,
                        camp_valid: camp | VALID,
                        id_meta: meta,
                    },
                    None => Rec { idhash, user: Key::default(), line: line_sp, ts: 0, camp_valid: 0, id_meta: meta },
                };
                recs.push(rec);
            }
            sort_by_bucket(recs, |r| bucket(r.idhash))
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
                let order = dedup_order(recs.len(), |i| recs[i].idhash);
                let emit = |r: &Rec, clicks: &mut Vec<Click>| {
                    if r.camp_valid & VALID != 0 {
                        let person = ptab.lookup(r.user);
                        let click_id = store.span_of(r.id(store));
                        clicks.push(Click { person, click_id, camp: r.camp_valid & !VALID, ts: r.ts });
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
                    // copies of one id (or a hash collision): order by (id, the oracle's copy order)
                    let mut g: Vec<(Row<'_, 4>, usize)> = grp
                        .iter()
                        .map(|&k| {
                            let mut f = [None; 4];
                            split_line(store.get(recs[k].line), &mut f);
                            (f, k)
                        })
                        .collect();
                    g.sort_unstable_by(|a, b| a.0[0].cmp(&b.0[0]).then_with(|| copy_order(&a.0, &b.0)));
                    for (n, (row, k)) in g.iter().enumerate() {
                        if n == 0 || g[n - 1].0[0] != row[0] {
                            emit(&recs[*k], clicks);
                        }
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
        .for_each(|v| group_by_person(v, |c| (c.person, c.ts, store.get(c.click_id))));
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
        assert_eq!(std::mem::size_of::<Rec>(), 48);
        assert_eq!(std::mem::size_of::<Click>(), 40);
    }
    #[test]
    fn click_id_from_the_line() {
        let d = b"c1,u,x,1\n\"c22\",u,x,1\n".to_vec();
        let store = Store::from_slices(vec![&d], crate::span::MAX_LEN);
        let (l1, l2) = (&d[0..8], &d[9..20]);
        let mk = |line: &[u8], id: &[u8], meta: Option<u32>| Rec {
            idhash: 0,
            user: Key::default(),
            line: store.span_of(&d[line.as_ptr() as usize - d.as_ptr() as usize..][..line.len()]),
            ts: 0,
            camp_valid: 0,
            id_meta: meta.unwrap_or_else(|| id_meta(line, id)),
        };
        assert_eq!(mk(l1, &l1[0..2], None).id(&store), b"c1");
        assert_eq!(mk(l2, &l2[1..4], None).id(&store), b"c22");
        assert_eq!(mk(l2, &l2[1..4], Some(RESPLIT)).id(&store), b"c22");
    }
}
