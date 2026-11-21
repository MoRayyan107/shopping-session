//! Stage 3: linked IDs (rules.md "Linked IDs"). Mirrors `pipeline/links.py::build_person_map`:
//! edges are non-NULL, non-self links; each connected component is named by its byte-wise smallest ID.
use crate::raw::Row;
use crate::util::{hash_bytes, FxMap};

#[derive(Clone, Copy)]
struct Slot<'a> {
    hash: u64,
    id: &'a [u8],
    person: &'a [u8],
    phash: u64,
}

/// The person map as an open-addressing table keyed by a precomputed hash of the ID, so a lookup for an
/// ID that is not renamed (most of them) usually never reads the ID's bytes.
pub struct PersonTable<'a> {
    mask: usize,
    slots: Vec<Option<Slot<'a>>>,
}

impl<'a> PersonTable<'a> {
    pub fn new(map: &FxMap<&'a [u8], &'a [u8]>) -> PersonTable<'a> {
        let cap = (map.len() * 2).next_power_of_two().max(16);
        let mut slots: Vec<Option<Slot<'a>>> = vec![None; cap];
        for (&id, &person) in map {
            let hash = hash_bytes(id);
            let mut i = hash as usize & (cap - 1);
            while slots[i].is_some() {
                i = (i + 1) & (cap - 1);
            }
            slots[i] = Some(Slot { hash, id, person, phash: hash_bytes(person) });
        }
        PersonTable { mask: cap - 1, slots }
    }

    /// (person, hash of person) for a user ID whose hash_bytes() is `uhash`.
    #[inline]
    pub fn lookup(&self, uhash: u64, user: &'a [u8]) -> (&'a [u8], u64) {
        let mut i = uhash as usize & self.mask;
        while let Some(s) = &self.slots[i] {
            if s.hash == uhash && s.id == user {
                return (s.person, s.phash);
            }
            i = (i + 1) & self.mask;
        }
        (user, uhash)
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

/// Map from every linked ID to its person (the smallest ID of its component), only for IDs that change.
pub fn person_map<'a>(links: &[Row<'a, 2>]) -> FxMap<&'a [u8], &'a [u8]> {
    let mut idx: FxMap<&'a [u8], u32> = FxMap::default();
    let mut ids: Vec<&'a [u8]> = Vec::new();
    let mut edges: Vec<(u32, u32)> = Vec::with_capacity(links.len());
    for l in links {
        let (Some(a), Some(b)) = (l[0], l[1]) else { continue };
        if a == b {
            continue;
        }
        let mut get = |s: &'a [u8]| {
            *idx.entry(s).or_insert_with(|| {
                ids.push(s);
                (ids.len() - 1) as u32
            })
        };
        let (ia, ib) = (get(a), get(b));
        edges.push((ia, ib));
    }
    let n = ids.len();
    let mut parent: Vec<u32> = (0..n as u32).collect();
    let mut size = vec![1u32; n];
    for &(a, b) in &edges {
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra != rb {
            let (big, small) = if size[ra as usize] >= size[rb as usize] { (ra, rb) } else { (rb, ra) };
            parent[small as usize] = big;
            size[big as usize] += size[small as usize];
        }
    }
    // smallest ID per root
    let mut best: Vec<u32> = (0..n as u32).collect();
    let roots: Vec<u32> = (0..n as u32).map(|i| find(&mut parent, i)).collect();
    for i in 0..n {
        let r = roots[i] as usize;
        if ids[i] < ids[best[r] as usize] {
            best[r] = i as u32;
        }
    }
    let mut map = FxMap::with_capacity_and_hasher(n, Default::default());
    for i in 0..n {
        let p = ids[best[roots[i] as usize] as usize];
        if p != ids[i] {
            map.insert(ids[i], p);
        }
    }
    map
}
