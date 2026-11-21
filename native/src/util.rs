//! Small shared helpers: a fast hasher for byte keys, byte-wise comparison with NULLs last, and
//! hash-partitioning of row indices.
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use rayon::prelude::*;

/// FxHash-style hasher (rotate, xor, multiply), fine for in-memory hash maps keyed by IDs.
#[derive(Default, Clone, Copy)]
pub struct FxHasher {
    h: u64,
}

const K: u64 = 0x517c_c1b7_2722_0a95;

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            self.add(u64::from_le_bytes(c.try_into().unwrap()));
        }
        let rest = chunks.remainder();
        if !rest.is_empty() {
            let mut b = [0u8; 8];
            b[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(b) ^ ((rest.len() as u64) << 59));
        }
    }
    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.add(i as u64);
    }
    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.add(i as u64);
    }
    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.add(i);
    }
    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.add(i as u64);
    }
    #[inline]
    fn finish(&self) -> u64 {
        // final avalanche so the top bits (used for partitioning) are well mixed
        let mut x = self.h;
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
        x ^= x >> 33;
        x
    }
}

impl FxHasher {
    #[inline(always)]
    fn add(&mut self, w: u64) {
        self.h = (self.h.rotate_left(5) ^ w).wrapping_mul(K);
    }
}

pub type FxBuild = BuildHasherDefault<FxHasher>;
pub type FxMap<K, V> = HashMap<K, V, FxBuild>;

#[inline]
pub fn hash_bytes(b: &[u8]) -> u64 {
    let mut h = FxHasher::default();
    h.write(b);
    h.write_usize(b.len());
    h.finish()
}

/// Byte-wise order of optional values with NULLs last (DuckDB's default ORDER BY).
#[inline]
pub fn cmp_nulls_last(a: Option<&[u8]>, b: Option<&[u8]>) -> Ordering {
    match (a, b) {
        (Some(x), Some(y)) => x.cmp(y),
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
    }
}

/// With NATIVE_PROFILE set, log a sub-step's time to stderr.
pub fn prof(label: &str, since: &mut std::time::Instant) {
    if std::env::var_os("NATIVE_PROFILE").is_some() {
        eprintln!("            {:7.3}s  {label}", since.elapsed().as_secs_f64());
    }
    *since = std::time::Instant::now();
}

pub const PART_BITS: u32 = 8;

const IDX_BITS: u32 = 24;

/// Indices 0..n sorted so that equal ids are adjacent: keys are 40 bits of the id hash (below the bucket
/// bits) and a 24-bit index. Equal truncated hashes form one group; callers resolve a group by comparing the
/// real ids, so a truncated-hash collision costs only a comparison, never a wrong merge.
pub fn dedup_order(n: usize, idhash: impl Fn(usize) -> u64) -> Vec<u64> {
    assert!(n < 1 << IDX_BITS, "a dedup bucket holds {n} rows, more than 2^{IDX_BITS}");
    let mut keys: Vec<u64> = (0..n).map(|i| (idhash(i) << PART_BITS) & !((1u64 << IDX_BITS) - 1) | i as u64).collect();
    keys.sort_unstable();
    keys
}

/// The same answer as `dedup_groups(&dedup_order(n, idhash))`, by hashing instead of sorting: which rows are
/// alone in their group, and the groups of 2 or more (rows with equal truncated id hash, the same 40 bits
/// as `dedup_order`'s keys), each group in row order. `table` is scratch, reused across calls.
/// An open-addressing table of (truncated hash, first row + 1) finds each row's group in one probe sequence;
/// later rows of a group are only listed, so the table never holds two slots of one group.
pub fn dedup_find(n: usize, idhash: impl Fn(usize) -> u64, table: &mut Vec<u64>) -> (Vec<bool>, Vec<Vec<usize>>) {
    let mut g = Groups::default();
    dedup_find_into(n, idhash, table, &mut g);
    let groups = (0..g.len()).map(|i| g.group(i).iter().map(|&k| k as usize).collect()).collect();
    (g.alone, groups)
}

/// The answer of `dedup_find` in reused buffers: `alone[i]`, and the groups as one flat list of rows.
#[derive(Default)]
pub struct Groups {
    pub alone: Vec<bool>,
    rows: Vec<u32>,
    starts: Vec<u32>,
    later: Vec<(u32, u32)>,
}

impl Groups {
    pub fn len(&self) -> usize {
        self.starts.len().saturating_sub(1)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Group i's rows (in row order until the caller reorders them).
    pub fn group(&mut self, i: usize) -> &mut [u32] {
        &mut self.rows[self.starts[i] as usize..self.starts[i + 1] as usize]
    }
}

/// `dedup_find` into `g`, with no allocation once the buffers have grown.
pub fn dedup_find_into(n: usize, idhash: impl Fn(usize) -> u64, table: &mut Vec<u64>, g: &mut Groups) {
    assert!(n < (1 << IDX_BITS) - 1, "a dedup bucket holds {n} rows, more than 2^{IDX_BITS} - 2");
    let mask = (1u64 << IDX_BITS) - 1;
    let bits = ((2 * n).max(16)).next_power_of_two().trailing_zeros();
    let cap = 1usize << bits;
    table.clear();
    table.resize(cap, 0);
    // (first row of the group, a later row) for every row whose truncated hash was seen before
    let later = &mut g.later;
    later.clear();
    for i in 0..n {
        // the same 40 bits as dedup_order: below the bucket bits, above the index bits
        let h = (idhash(i) << PART_BITS) & !mask;
        let mut pos = (h.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - bits)) as usize;
        loop {
            let s = table[pos];
            if s == 0 {
                table[pos] = h | (i as u64 + 1);
                break;
            }
            if s & !mask == h {
                later.push((((s & mask) - 1) as u32, i as u32));
                break;
            }
            pos = (pos + 1) & (cap - 1);
        }
    }
    g.alone.clear();
    g.alone.resize(n, true);
    g.rows.clear();
    g.starts.clear();
    if later.is_empty() {
        return;
    }
    // later rows are listed in row order, so a stable sort by first row keeps each group in row order
    later.sort_by_key(|p| p.0);
    let mut a = 0;
    while a < later.len() {
        let first = later[a].0;
        g.starts.push(g.rows.len() as u32);
        g.rows.push(first);
        g.alone[first as usize] = false;
        while a < later.len() && later[a].0 == first {
            g.rows.push(later[a].1);
            g.alone[later[a].1 as usize] = false;
            a += 1;
        }
    }
    g.starts.push(g.rows.len() as u32);
}

/// From `order` (dedup_order of n rows): which rows are alone in their group, and the groups of 2 or more.
pub fn dedup_groups(order: &[u64]) -> (Vec<bool>, Vec<Vec<usize>>) {
    let mask = (1u64 << IDX_BITS) - 1;
    let mut alone = vec![true; order.len()];
    let mut groups = Vec::new();
    let mut i = 0;
    while i < order.len() {
        let h = order[i] & !mask;
        let mut j = i + 1;
        while j < order.len() && order[j] & !mask == h {
            j += 1;
        }
        if j > i + 1 {
            let g: Vec<usize> = order[i..j].iter().map(|k| (k & mask) as usize).collect();
            for &k in &g {
                alone[k] = false;
            }
            groups.push(g);
        }
        i = j;
    }
    (alone, groups)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_groups_with_forced_collisions() {
        // hashes: rows 0, 2, 5 collide (same hash), 1 and 4 collide only in bits that are dropped, 3 alone
        let h = [0xAAu64 << 20, 1 << 60, 0xAA << 20, 0x55 << 20, (1 << 60) | 0xFFFF, 0xAA << 20];
        let order = dedup_order(h.len(), |i| h[i]);
        let (alone, mut groups) = dedup_groups(&order);
        for g in &mut groups {
            g.sort();
        }
        groups.sort();
        // bits below 16 and the top 8 bits are not part of the key, so 1 and 4 form a group too
        assert_eq!(groups, vec![vec![0, 2, 5], vec![1, 4]]);
        assert_eq!(alone, vec![false, false, false, true, false, false]);
    }

    /// dedup_find gives exactly dedup_groups(dedup_order(..)): the same alone flags and the same groups,
    /// each in row order (groups compared as a sorted list).
    fn same_as_sorting(h: &[u64], table: &mut Vec<u64>) {
        let (alone_s, mut groups_s) = dedup_groups(&dedup_order(h.len(), |i| h[i]));
        let (alone_f, mut groups_f) = dedup_find(h.len(), |i| h[i], table);
        assert_eq!(alone_s, alone_f);
        for g in &groups_f {
            assert!(g.windows(2).all(|w| w[0] < w[1]), "a group in row order");
        }
        groups_s.sort();
        groups_f.sort();
        assert_eq!(groups_s, groups_f);
    }

    #[test]
    fn dedup_find_equals_sorting() {
        let mut table = Vec::new();
        // the forced collisions of the test above, and the empty case
        same_as_sorting(&[0xAAu64 << 20, 1 << 60, 0xAA << 20, 0x55 << 20, (1 << 60) | 0xFFFF, 0xAA << 20], &mut table);
        same_as_sorting(&[], &mut table);
        same_as_sorting(&[7], &mut table);
        // random hashes drawn from small pools: many groups, groups of every size, bits that are dropped
        // (top 8 bits, low 16 bits) varying inside a group, and long probe runs in a full table
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for &(n, pool) in &[(10usize, 3u64), (1000, 900), (5000, 100), (20000, 19000), (50000, 50000), (3000, 1)] {
            let base: Vec<u64> = (0..pool).map(|_| rnd()).collect();
            let h: Vec<u64> = (0..n)
                .map(|_| {
                    let b = base[(rnd() % pool) as usize];
                    // vary the bits outside the 40 compared ones
                    (b & 0x00FF_FFFF_FFFF_0000) | (rnd() & 0xFF00_0000_0000_FFFF)
                })
                .collect();
            same_as_sorting(&h, &mut table);
        }
    }

    #[test]
    fn bucket_sort_and_gather_keep_every_row_in_block_order() {
        // three blocks of (bucket, row id); buckets at both ends of the range, one block empty
        let blocks: Vec<Vec<(usize, u32)>> = vec![
            vec![(255, 0), (0, 1), (7, 2), (0, 3), (255, 4)],
            vec![],
            vec![(7, 5), (0, 6), (128, 7)],
        ];
        let sorted: Vec<(Vec<(usize, u32)>, Vec<u32>)> = blocks.iter().map(|b| sort_by_bucket(b, |r| r.0)).collect();
        for (s, start) in &sorted {
            assert_eq!(start.len(), NPARTS + 1);
            assert_eq!(*start.last().unwrap() as usize, s.len());
            assert!(s.windows(2).all(|w| w[0].0 < w[1].0 || (w[0].0 == w[1].0 && w[0].1 < w[1].1)), "stable");
        }
        let bk = Buckets::new(sorted);
        assert_eq!(bk.len(), 8);
        let mut g = Vec::new();
        bk.gather(0, &mut g);
        assert_eq!(g.iter().map(|r| r.1).collect::<Vec<_>>(), vec![1, 3, 6]);
        assert_eq!(bk.bucket_len(255), 2);
        assert_eq!(bk.bucket_len(1), 0);
        let vecs = bk.into_vecs();
        assert_eq!(vecs.len(), NPARTS);
        assert_eq!(vecs[7].iter().map(|r| r.1).collect::<Vec<_>>(), vec![2, 5]);
        assert_eq!(vecs[128], vec![(128, 7)]);
        assert_eq!(vecs.iter().map(Vec::len).sum::<usize>(), 8);
    }

    #[test]
    fn scatter_and_chunks_keep_every_row_of_a_bucket_in_push_order() {
        // rows (bucket, id) over three scatters, enough to fill many chunks of some buckets
        let mut scatters: Vec<Scatter<(u8, u32)>> = (0..3).map(|_| Scatter::default()).collect();
        let mut want: Vec<Vec<(u8, u32)>> = vec![Vec::new(); NPARTS];
        let mut id = 0u32;
        for round in 0..3 {
            for s in scatters.iter_mut() {
                for i in 0..20_000u32 {
                    let b = if i % 3 == 0 { 7 } else { ((i * 2654435761u32) >> 24) as usize };
                    s.push(b, (b as u8, id));
                    id += 1;
                }
            }
            let _ = round;
        }
        // per scatter, push order; scatters are concatenated in order
        let mut per: Vec<Vec<Vec<(u8, u32)>>> = vec![vec![Vec::new(); NPARTS]; 3];
        let mut id = 0u32;
        for _ in 0..3 {
            for p in per.iter_mut() {
                for i in 0..20_000u32 {
                    let b = if i % 3 == 0 { 7 } else { ((i * 2654435761u32) >> 24) as usize };
                    p[b].push((b as u8, id));
                    id += 1;
                }
            }
        }
        for p in &per {
            for b in 0..NPARTS {
                want[b].extend_from_slice(&p[b]);
            }
        }
        let ch = Chunks::new(scatters);
        assert_eq!(ch.len(), 180_000);
        assert_eq!(ch.to_vec().len(), 180_000);
        for b in 0..NPARTS {
            let mut got = Vec::new();
            ch.take(b, &mut got);
            assert_eq!(got, want[b], "bucket {b}");
            assert!(got.iter().all(|r| r.0 as usize == b));
        }
        assert!(ch.is_empty(), "every bucket was taken");
    }

    #[test]
    fn for_each_in_order_visits_every_item_once() {
        let items: Vec<u32> = (0..10_000).collect();
        let states = for_each_in_order(&items, Vec::new, |v: &mut Vec<u32>, &x| v.push(x));
        let mut all: Vec<u32> = states.into_iter().flatten().collect();
        all.sort();
        assert_eq!(all, items);
    }

    #[test]
    fn partition_keeps_every_index_once_in_order() {
        let n = 200_000;
        let parts = partition(n, |i| if i % 7 == 0 { None } else { Some(hash_bytes(&(i as u64).to_le_bytes())) });
        let mut all: Vec<u32> = parts.iter().flatten().copied().collect();
        assert!(parts.iter().all(|p| p.windows(2).all(|w| w[0] < w[1])));
        all.sort();
        assert_eq!(all, (0..n as u32).filter(|i| i % 7 != 0).collect::<Vec<_>>());
    }
}

pub const NPARTS: usize = 1 << PART_BITS;

/// `f` over `items` on every pool thread, handing items out strictly in order (a shared counter), so the
/// work moves through the input front to back, behind the read-ahead. Results are in item order, exactly
/// as `items.par_iter().map_init(init, f).collect()` would give them.
pub fn map_in_order<T, S, R, I, F>(items: &[T], init: I, f: F) -> Vec<R>
where
    T: Sync,
    R: Send + Sync,
    I: Fn() -> S + Sync,
    F: Fn(&mut S, &T) -> R + Sync,
{
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::OnceLock;
    let out: Vec<OnceLock<R>> = (0..items.len()).map(|_| OnceLock::new()).collect();
    let next = AtomicUsize::new(0);
    rayon::scope(|s| {
        for _ in 0..rayon::current_num_threads() {
            s.spawn(|_| {
                let mut state = init();
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= items.len() {
                        break;
                    }
                    let r = f(&mut state, &items[i]);
                    assert!(out[i].set(r).is_ok(), "item {i} done twice");
                }
            });
        }
    });
    out.into_iter().map(|c| c.into_inner().expect("every item is done")).collect()
}

/// `f` over `items` on every pool thread with one state per thread, handing items out strictly in order
/// (a shared counter, as `map_in_order`); returns every thread's final state (in no particular order).
pub fn for_each_in_order<T, S, I, F>(items: &[T], init: I, f: F) -> Vec<S>
where
    T: Sync,
    S: Send,
    I: Fn() -> S + Sync,
    F: Fn(&mut S, &T) + Sync,
{
    use std::sync::atomic::{AtomicUsize, Ordering};
    let next = AtomicUsize::new(0);
    let states = std::sync::Mutex::new(Vec::new());
    rayon::scope(|s| {
        for _ in 0..rayon::current_num_threads() {
            s.spawn(|_| {
                let mut state = init();
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= items.len() {
                        break;
                    }
                    f(&mut state, &items[i]);
                }
                states.lock().unwrap().push(state);
            });
        }
    });
    states.into_inner().unwrap()
}

/// About this many bytes per chunk of a `Scatter`.
const CHUNK_BYTES: usize = 32 << 10;

/// One thread's rows, written straight into per-bucket chunks (0..NPARTS), each chunk a Vec of a fixed
/// capacity; a full chunk is set aside and a new one started. Rows of a bucket keep their push order.
pub struct Scatter<T> {
    cur: Vec<Vec<T>>,
    full: Vec<Vec<Vec<T>>>,
}

impl<T> Default for Scatter<T> {
    fn default() -> Self {
        Scatter { cur: (0..NPARTS).map(|_| Vec::new()).collect(), full: (0..NPARTS).map(|_| Vec::new()).collect() }
    }
}

impl<T> Scatter<T> {
    #[inline(always)]
    pub fn push(&mut self, b: usize, x: T) {
        if self.cur[b].len() == self.cur[b].capacity() {
            self.next_chunk(b);
        }
        self.cur[b].push(x);
    }
    #[cold]
    #[inline(never)]
    fn next_chunk(&mut self, b: usize) {
        let rows = (CHUNK_BYTES / std::mem::size_of::<T>().max(1)).max(16);
        let c = std::mem::replace(&mut self.cur[b], Vec::with_capacity(rows));
        if !c.is_empty() {
            self.full[b].push(c);
        }
    }
}

/// The rows of many `Scatter`s by bucket: bucket b is a list of chunks. A bucket can be taken out (and freed
/// by its user) while the others stay, so each bucket's memory goes as soon as it is used.
pub struct Chunks<T> {
    buckets: Vec<std::sync::Mutex<Vec<Vec<T>>>>,
}

impl<T: Copy + Send> Chunks<T> {
    pub fn new(scatters: Vec<Scatter<T>>) -> Chunks<T> {
        let mut buckets: Vec<Vec<Vec<T>>> = (0..NPARTS).map(|_| Vec::new()).collect();
        for s in scatters {
            for (b, (full, cur)) in s.full.into_iter().zip(s.cur).enumerate() {
                buckets[b].extend(full);
                if !cur.is_empty() {
                    buckets[b].push(cur);
                }
            }
        }
        Chunks { buckets: buckets.into_iter().map(std::sync::Mutex::new).collect() }
    }
    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.lock().unwrap().iter().map(Vec::len).sum::<usize>()).sum()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Bucket b taken out of `self` and appended to `out` (exact capacity when `out` is empty); its chunks
    /// are freed.
    pub fn take(&self, b: usize, out: &mut Vec<T>) {
        let chunks = std::mem::take(&mut *self.buckets[b].lock().unwrap());
        out.reserve_exact(chunks.iter().map(Vec::len).sum());
        for c in chunks {
            out.extend_from_slice(&c);
        }
    }
    /// Every row still held, bucket by bucket (dumps only).
    pub fn to_vec(&self) -> Vec<T> {
        let mut v = Vec::new();
        for b in &self.buckets {
            for c in b.lock().unwrap().iter() {
                v.extend_from_slice(c);
            }
        }
        v
    }
}

/// Rows of many blocks, each block ordered by bucket (0..NPARTS) with the start of every bucket, so bucket
/// b is the run of slices blocks[k][start[b]..start[b+1]] in block order. Nothing is copied to build it.
pub struct Buckets<T> {
    blocks: Vec<(Vec<T>, Vec<u32>)>,
}

impl<T: Copy + Send + Sync> Buckets<T> {
    pub fn new(blocks: Vec<(Vec<T>, Vec<u32>)>) -> Buckets<T> {
        Buckets { blocks }
    }
    pub fn len(&self) -> usize {
        self.blocks.iter().map(|b| b.0.len()).sum()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn bucket_len(&self, b: usize) -> usize {
        self.blocks.iter().map(|(_, s)| (s[b + 1] - s[b]) as usize).sum()
    }
    /// Bucket b appended to `out`, in block order.
    pub fn gather(&self, b: usize, out: &mut Vec<T>) {
        out.reserve(self.bucket_len(b));
        for (v, s) in &self.blocks {
            out.extend_from_slice(&v[s[b] as usize..s[b + 1] as usize]);
        }
    }
    /// Every bucket as one Vec (exact size), in parallel; the blocks are freed afterwards.
    pub fn into_vecs(self) -> Vec<Vec<T>> {
        let out = (0..NPARTS)
            .into_par_iter()
            .map(|b| {
                let mut v = Vec::new();
                self.gather(b, &mut v);
                v
            })
            .collect();
        drop(self);
        out
    }
}

/// Stable counting sort of `v` by bucket (0..NPARTS): the rows in bucket order and each bucket's start.
pub fn sort_by_bucket<T: Copy>(v: &[T], bucket: impl Fn(&T) -> usize) -> (Vec<T>, Vec<u32>) {
    let mut start = vec![0u32; NPARTS + 1];
    if v.is_empty() {
        return (Vec::new(), start);
    }
    assert!(v.len() < u32::MAX as usize, "a block holds {} rows", v.len());
    let ids: Vec<u8> = v.iter().map(|x| bucket(x) as u8).collect();
    for &b in &ids {
        start[b as usize + 1] += 1;
    }
    for i in 0..NPARTS {
        start[i + 1] += start[i];
    }
    let mut pos = start.clone();
    let mut out = vec![v[0]; v.len()];
    for (x, &b) in v.iter().zip(&ids) {
        out[pos[b as usize] as usize] = *x;
        pos[b as usize] += 1;
    }
    (out, start)
}

/// blocks[b][p] -> out[p] = blocks[0][p] ++ blocks[1][p] ++ ... (parallel over p)
pub fn transpose_concat<T: Send>(blocks: Vec<Vec<Vec<T>>>) -> Vec<Vec<T>> {
    let nparts = blocks.first().map_or(1usize << PART_BITS, Vec::len);
    let mut t: Vec<Vec<Vec<T>>> = (0..nparts).map(|_| Vec::with_capacity(blocks.len())).collect();
    for b in blocks {
        assert_eq!(b.len(), nparts);
        for (p, v) in b.into_iter().enumerate() {
            t[p].push(v);
        }
    }
    t.into_par_iter()
        .map(|vs| {
            let mut out = Vec::with_capacity(vs.iter().map(Vec::len).sum());
            for v in vs {
                out.extend(v);
            }
            out
        })
        .collect()
}

/// Group row indices by the top PART_BITS bits of `hash(i)`: returns one Vec of indices per partition.
pub fn partition<F>(n: usize, hash: F) -> Vec<Vec<u32>>
where
    F: Fn(usize) -> Option<u64> + Sync,
{
    const BLOCK: usize = 1 << 16;
    let parts = 1usize << PART_BITS;
    // pass 1: every block of indices split into its own buckets (indices ascending within a bucket)
    let blocks: Vec<Vec<Vec<u32>>> = (0..n.div_ceil(BLOCK))
        .into_par_iter()
        .map(|b| {
            let mut local: Vec<Vec<u32>> = (0..parts).map(|_| Vec::with_capacity(BLOCK / parts * 2)).collect();
            for i in b * BLOCK..((b + 1) * BLOCK).min(n) {
                if let Some(h) = hash(i) {
                    local[(h >> (64 - PART_BITS)) as usize].push(i as u32);
                }
            }
            local
        })
        .collect();
    // pass 2: every bucket joins its pieces in block order, so each bucket stays in index order
    (0..parts)
        .into_par_iter()
        .map(|p| {
            let mut v = Vec::with_capacity(blocks.iter().map(|b| b[p].len()).sum());
            for b in &blocks {
                v.extend_from_slice(&b[p]);
            }
            v
        })
        .collect()
}
