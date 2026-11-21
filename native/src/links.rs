//! Stage 3: linked IDs (rules.md "Linked IDs"). Mirrors `pipeline/links.py::build_person_map`:
//! edges are non-NULL, non-self links; each connected component is named by its byte-wise smallest ID.
//! IDs are exact keys (`key::Key`), interned in parallel by hash partition; the union-find runs on integers.
use std::cmp::Ordering;

use rayon::prelude::*;

use crate::key::Key;
use crate::raw::Row;
use crate::span::Store;
use crate::util::{partition, FxMap};

/// One renamed ID (32 bytes); `id == Key::default()` (the empty ID, never renamed) marks an empty slot.
#[derive(Clone, Copy, Default)]
struct Slot {
    id: Key,
    person: Key,
}

/// The person map as an open-addressing table keyed by exact ID keys (`key::Key`), so a lookup never reads
/// the input files; a bit filter in front answers "not renamed" (most IDs) from one cache line.
pub struct PersonTable {
    mask: usize,
    slots: Vec<Slot>,
    /// one bit per (hash >> 32) & bit_mask: set for every renamed ID; a clear bit means "not renamed"
    bits: Vec<u64>,
    bit_mask: usize,
}

impl PersonTable {
    /// From the renamed IDs as (id, person) pairs (`person_pairs`).
    pub fn new(pairs: &[(Key, Key)]) -> PersonTable {
        let cap = (pairs.len() * 2).next_power_of_two().max(16);
        let nbits = (pairs.len() * 16).next_power_of_two().max(64);
        let mut slots: Vec<Slot> = vec![Slot::default(); cap];
        let mut bits = vec![0u64; nbits / 64];
        for &(key, person) in pairs {
            // renamed IDs are never empty (empty link sides are dropped), so a used slot has id != the empty key
            assert!(key != Key::default());
            let hash = key.hash();
            let b = (hash >> 32) as usize & (nbits - 1);
            bits[b / 64] |= 1 << (b % 64);
            let mut i = hash as usize & (cap - 1);
            while slots[i].id != Key::default() {
                i = (i + 1) & (cap - 1);
            }
            slots[i] = Slot { id: key, person };
        }
        PersonTable { mask: cap - 1, slots, bits, bit_mask: nbits - 1 }
    }

    /// The person of a (non-empty) user ID.
    #[inline]
    pub fn lookup(&self, user: Key) -> Key {
        let hash = user.hash();
        let b = (hash >> 32) as usize & self.bit_mask;
        if self.bits[b / 64] & (1 << (b % 64)) == 0 {
            return user;
        }
        let mut i = hash as usize & self.mask;
        loop {
            let s = &self.slots[i];
            if s.id == user {
                return s.person;
            }
            if s.id == Key::default() {
                return user;
            }
            i = (i + 1) & self.mask;
        }
    }
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let p = parent[parent[x as usize] as usize];
        parent[x as usize] = p;
        x = p;
    }
    x
}

/// Byte-wise order of two IDs by key: inline keys order exactly as their bytes; a long key is read back.
fn cmp_ids(store: &Store<'_>, a: Key, b: Key) -> Ordering {
    if a.is_inline() && b.is_inline() {
        a.cmp(&b)
    } else {
        a.to_vec(store).cmp(&b.to_vec(store))
    }
}

/// Every linked ID that changes, with its person (the byte-wise smallest ID of its component), as
/// (id, person) pairs in no particular order.
pub fn person_pairs<'a>(links: &[Row<'a, 2>], store: &Store<'a>) -> Vec<(Key, Key)> {
    // the edges as keys: both sides present and different (equal keys <=> equal IDs)
    let edges: Vec<(Key, Key)> = links
        .par_iter()
        .filter_map(|l| {
            let (a, b) = (store.key_of(l[0]?), store.key_of(l[1]?));
            (a != b).then_some((a, b))
        })
        .collect();
    // intern: every side goes to a partition by hash; each partition numbers its distinct keys on its own
    let side = |i: usize| if i % 2 == 0 { edges[i / 2].0 } else { edges[i / 2].1 };
    let parts = partition(edges.len() * 2, |i| Some(side(i).hash()));
    let local: Vec<(Vec<Key>, Vec<u32>)> = parts
        .par_iter()
        .map(|idx| {
            let mut m: FxMap<Key, u32> = FxMap::with_capacity_and_hasher(idx.len(), Default::default());
            let mut keys = Vec::new();
            let ids = idx
                .iter()
                .map(|&i| {
                    *m.entry(side(i as usize)).or_insert_with(|| {
                        keys.push(side(i as usize));
                        (keys.len() - 1) as u32
                    })
                })
                .collect();
            (keys, ids)
        })
        .collect();
    let mut base = Vec::with_capacity(local.len());
    let mut n = 0u32;
    for (keys, _) in &local {
        base.push(n);
        n += keys.len() as u32;
    }
    let mut side_id = vec![0u32; edges.len() * 2];
    for ((idx, (_, ids)), &b) in parts.iter().zip(&local).zip(&base) {
        for (&i, &id) in idx.iter().zip(ids) {
            side_id[i as usize] = b + id;
        }
    }
    let ids: Vec<Key> = local.into_iter().flat_map(|(keys, _)| keys).collect();
    let n = n as usize;
    // union by size over the integer ids
    let mut parent: Vec<u32> = (0..n as u32).collect();
    let mut size = vec![1u32; n];
    for e in 0..edges.len() {
        let (ra, rb) = (find(&mut parent, side_id[2 * e]), find(&mut parent, side_id[2 * e + 1]));
        if ra != rb {
            let (big, small) = if size[ra as usize] >= size[rb as usize] { (ra, rb) } else { (rb, ra) };
            parent[small as usize] = big;
            size[big as usize] += size[small as usize];
        }
    }
    // smallest ID per root
    let roots: Vec<u32> = (0..n as u32).map(|i| find(&mut parent, i)).collect();
    let mut best: Vec<u32> = (0..n as u32).collect();
    for i in 0..n {
        let r = roots[i] as usize;
        if cmp_ids(store, ids[i], ids[best[r] as usize]) == Ordering::Less {
            best[r] = i as u32;
        }
    }
    (0..n)
        .into_par_iter()
        .filter_map(|i| {
            let p = ids[best[roots[i] as usize] as usize];
            (p != ids[i]).then_some((ids[i], p))
        })
        .collect()
}


#[cfg(test)]
mod tests {
    use super::*;

    fn run(rows: &[(&str, &str)], inline_max: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let text: Vec<u8> = rows.iter().flat_map(|(a, b)| format!("{a},{b}\n").into_bytes()).collect();
        let store = Store::from_slices_with_inline(vec![&text], crate::span::MAX_LEN, inline_max);
        let mut links: Vec<Row<'_, 2>> = Vec::new();
        for line in text.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
            let mut f = [None; 2];
            assert!(crate::csv::split_line(line, &mut f));
            links.push(f);
        }
        let pairs = person_pairs(&links, &store);
        let table = PersonTable::new(&pairs);
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = pairs.iter().map(|(a, b)| (a.to_vec(&store), b.to_vec(&store))).collect();
        out.sort();
        // the table answers every renamed ID with its person and every other ID with itself
        for (id, person) in &out {
            let k = store.key_of(&text[find_at(&text, id)..][..id.len()]);
            assert_eq!(table.lookup(k).to_vec(&store), *person);
        }
        assert_eq!(table.lookup(Key::inline(b"nobody")), Key::inline(b"nobody"));
        out
    }

    fn find_at(hay: &[u8], needle: &[u8]) -> usize {
        hay.windows(needle.len()).position(|w| w == needle).unwrap()
    }

    #[test]
    fn components_named_by_smallest_id() {
        // a chain d-c-b, a separate pair, a self link, repeats both ways round, an empty side
        let rows = [("d", "c"), ("c", "b"), ("x2", "x1"), ("s", "s"), ("b", "c"), ("c", "d"), ("q", "")];
        for inline_max in [crate::key::INLINE_MAX, 0] {
            let got = run(&rows, inline_max);
            let want: Vec<(Vec<u8>, Vec<u8>)> =
                [("c", "b"), ("d", "b"), ("x2", "x1")].iter().map(|(a, b)| (a.as_bytes().to_vec(), b.as_bytes().to_vec())).collect();
            assert_eq!(got, want, "inline_max {inline_max}");
        }
    }

    #[test]
    fn byte_order_names_with_long_and_short_ids() {
        // 20-byte IDs (long keys) mixed with short ones: the name is the byte-wise smallest, whatever the path
        let long_a = "aaaaaaaaaaaaaaaaaaaa";
        let long_b = "aaaaaaaaaaaaaaaaaaab";
        let rows = [(long_b, "b"), ("b", long_a), ("zz", long_b)];
        for inline_max in [crate::key::INLINE_MAX, 1] {
            let got = run(&rows, inline_max);
            assert!(got.iter().all(|(_, p)| p == long_a.as_bytes()), "{got:?}");
            assert_eq!(got.len(), 3);
        }
    }
}
