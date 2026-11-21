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
