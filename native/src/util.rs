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

pub const PART_BITS: u32 = 8;

/// Group row indices by the top PART_BITS bits of `hash(i)`: returns one Vec of indices per partition.
pub fn partition<F>(n: usize, hash: F) -> Vec<Vec<u32>>
where
    F: Fn(usize) -> Option<u64> + Sync,
{
    let parts = 1usize << PART_BITS;
    let mut keys: Vec<u64> = (0..n)
        .into_par_iter()
        .filter_map(|i| hash(i).map(|h| ((h >> (64 - PART_BITS)) << 32) | i as u64))
        .collect();
    keys.par_sort_unstable();
    let mut out = vec![Vec::new(); parts];
    let mut start = 0;
    while start < keys.len() {
        let p = (keys[start] >> 32) as usize;
        let mut end = start;
        while end < keys.len() && (keys[end] >> 32) as usize == p {
            end += 1;
        }
        out[p] = keys[start..end].iter().map(|k| *k as u32).collect();
        start = end;
    }
    out
}
