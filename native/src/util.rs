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
