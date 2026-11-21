//! Stage 6: clicks dedup, validation, person, credit and assists (rules.md "Clicks").
//! Mirrors `pipeline/clicks.py`.
use std::cmp::Ordering;

use rayon::prelude::*;

use crate::csv::{for_each_row, split_line};
use crate::key::Key;
use crate::links::{PersonTable, PREFETCH_ROWS};
use crate::raw::{pieces_with_file, Mapped, Row};
use crate::sessions::Part;
use crate::span::{Span, Store};
use crate::ts::parse_ts;
use crate::util::{cmp_nulls_last, dedup_find_into, for_each_in_order, Groups, Chunks, FxMap, Scatter, NPARTS, PART_BITS};

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
    /// assisted sessions per (campaign, day)
    pub assist_counts: Counts,
}

/// The (campaign, day) cells of campaign_daily: every session's day lies in [day0, day0 + ndays) (sessions
/// start inside the window). Counted in dense arrays (cell = camp * ndays + day - day0) when the cells are
/// few (NATIVE_DENSE_MAX_CELLS, default 2^17; 0 forces the maps), else in hash maps keyed by `key`.
#[derive(Clone, Copy)]
pub struct Cells {
    pub day0: i32,
    pub ndays: usize,
    pub ncamp: usize,
    pub dense: bool,
}

impl Cells {
    pub fn new(ncamp: usize, ws: i64, we: i64) -> Cells {
        let day0 = crate::sessions::day_of(ws);
        let ndays = (crate::sessions::day_of(we.max(ws + 1) - 1) - day0 + 1).max(1) as usize;
        let max = std::env::var("NATIVE_DENSE_MAX_CELLS").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(1 << 17);
        Cells { day0, ndays, ncamp, dense: ncamp.saturating_mul(ndays) <= max }
    }
    pub fn len(&self) -> usize {
        self.ncamp * self.ndays
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// The dense index of (camp, day).
    #[inline(always)]
    pub fn cell(&self, camp: u32, day: i32) -> usize {
        let d = (day - self.day0) as usize;
        assert!(d < self.ndays, "session day {day} outside the window's days");
        camp as usize * self.ndays + d
    }
    /// (camp, day) of a dense index.
    pub fn of(&self, cell: usize) -> (u32, i32) {
        ((cell / self.ndays) as u32, self.day0 + (cell % self.ndays) as i32)
    }
}

/// Assisted-session counts per (campaign, day) of one part.
pub enum Counts {
    Dense(Vec<u32>),
    Map(FxMap<u64, i64>),
}

impl Counts {
    pub fn new(cells: &Cells) -> Counts {
        if cells.dense {
            Counts::Dense(vec![0; cells.len()])
        } else {
            Counts::Map(FxMap::default())
        }
    }
    #[inline(always)]
    fn add(&mut self, cells: &Cells, camp: u32, day: i32) {
        match self {
            Counts::Dense(v) => v[cells.cell(camp, day)] += 1,
            Counts::Map(m) => *m.entry(key(camp, day)).or_insert(0) += 1,
        }
    }
    pub fn total(&self) -> i64 {
        match self {
            Counts::Dense(v) => v.iter().map(|&n| n as i64).sum(),
            Counts::Map(m) => m.values().sum(),
        }
    }
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
    let bucket = |h: u64| (h >> (64 - PART_BITS)) as usize;
    assert!(camps.names.len() < VALID as usize, "too many campaigns");
    // pass 1: split + validate every line; each record goes straight to its click_id-hash bucket
    let scatters = for_each_in_order(&pieces_with_file(files, first), Scatter::default, |sc: &mut Scatter<Rec>, &(file, piece)| {
            crate::raw::note_taken(piece.len());
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
                sc.push(bucket(rec.id.hash()), rec);
            });
        });
    let by_id = Chunks::new(scatters);
    // pass 2: dedup each id bucket (freed once done); kept valid clicks go straight to their person bucket
    let ids: Vec<usize> = (0..NPARTS).collect();
    type State = (Vec<Rec>, Vec<u64>, Scatter<Click>, Groups, Vec<usize>);
    let states = for_each_in_order(
        &ids,
        || (Vec::new(), Vec::new(), Scatter::default(), Groups::default(), Vec::new()),
        |(recs, table, clicks, groups, gbuf): &mut State, &b| {
                recs.clear();
                by_id.take(b, recs);
                let emit = |r: &Rec, clicks: &mut Scatter<Click>| {
                    if r.camp_valid & VALID != 0 {
                        let person = ptab.lookup(r.user);
                        clicks.push(bucket(person.hash()), Click { person, click_id: r.id, camp: r.camp_valid & !VALID, ts: r.ts });
                    }
                };
                dedup_find_into(recs.len(), recs.iter().map(|r| r.id.hash()), table, groups);
                // unique ids in memory order (sequential reads), then the groups of copies
                // the person table is random access: its filter word is requested 2D rows ahead, its slot D
                const D: usize = PREFETCH_ROWS;
                for i in 0..recs.len() {
                    if let Some(r) = recs.get(i + 2 * D) {
                        ptab.prefetch_bits(r.user);
                    }
                    if let Some(r) = recs.get(i + D) {
                        ptab.prefetch_slot(r.user);
                    }
                    if groups.alone[i] {
                        emit(&recs[i], clicks);
                    }
                }
                for gi in 0..groups.len() {
                    // a truncated-hash group: runs of one click_id (equal keys <=> equal IDs)
                    gbuf.clear();
                    gbuf.extend(groups.group(gi).iter().map(|&k| k as usize));
                    let g = &mut *gbuf;
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
        },
    );
    drop(by_id);
    let by_person = Chunks::new(states.into_iter().map(|s| s.2).collect());
    (0..NPARTS)
        .into_par_iter()
        .map(|b| {
            let mut v = Vec::new();
            by_person.take(b, &mut v);
            sort_clicks(&mut v, store);
            v
        })
        .collect()
}

/// (person, ts) packed so that u128 order is exactly (person, ts) order.
#[inline(always)]
fn person_ts(c: &Click) -> u128 {
    ((c.person.raw() as u128) << 64) | ((c.ts as u64) ^ (1 << 63)) as u128
}

/// Sort by (person, ts, click_id byte-wise): one packed-key sort, then each run of equal (person, ts) by
/// click_id (click_ids are distinct after dedup, so the order is total and the sort's stability is moot).
fn sort_clicks(v: &mut [Click], store: &Store<'_>) {
    v.sort_unstable_by_key(person_ts);
    let mut a = 0;
    while a < v.len() {
        let mut z = a + 1;
        while z < v.len() && v[z].person == v[a].person && v[z].ts == v[a].ts {
            z += 1;
        }
        if z - a > 1 {
            v[a..z].sort_unstable_by(|x, y| cmp_click_ids(store, x.click_id, y.click_id));
        }
        a = z;
    }
}

/// Credit and assists for every session, part by part. Sessions and clicks of a part are both ordered by
/// person key, so one forward walk finds each person's clicks.
pub fn credit(parts: &[Part], clicks: &[Vec<Click>], camps: &Campaigns<'_>, cells: &Cells, keep_assists: bool) -> Vec<Credit> {
    assert_eq!(clicks.len(), parts.len());
    let ncamp = camps.names.len();
    parts
        .par_iter()
        .zip(clicks.par_iter())
        .map(|(p, ck)| {
            let mut out = Credit {
                credit: Vec::with_capacity(p.sessions.len()),
                assists: Vec::new(),
                assist_counts: Counts::new(cells),
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
                        out.assist_counts.add(cells, c.camp, s.day);
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
    fn packed_sort_equals_the_comparator_sort() {
        // persons and timestamps from tiny sets (many ties, negative ts), click_ids short and long
        let ids: Vec<String> = (0..400).map(|i| if i % 5 == 0 { format!("{:020}", 999 - i) } else { format!("c{}", 999 - i) }).collect();
        let d = ids.join(",").into_bytes();
        let store = Store::from_slices(vec![&d], crate::span::MAX_LEN);
        let keys: Vec<Key> = d.split(|&b| b == b',').map(|f| store.key(0, f)).collect();
        let persons = [Key::inline(b"a"), Key::inline(b"b"), Key::inline(b"zz"), Key::default()];
        let tss = [i64::MIN, -5, 0, 7, i64::MAX];
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        let mut v: Vec<Click> = keys
            .iter()
            .map(|&k| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                Click { person: persons[(x % 4) as usize], click_id: k, ts: tss[((x >> 8) % 5) as usize], camp: 0 }
            })
            .collect();
        let mut want = v.clone();
        want.sort_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)).then_with(|| cmp_click_ids(&store, a.click_id, b.click_id)));
        sort_clicks(&mut v, &store);
        let tuple = |c: &Click| (c.person, c.ts, c.click_id);
        assert_eq!(v.iter().map(tuple).collect::<Vec<_>>(), want.iter().map(tuple).collect::<Vec<_>>());
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
