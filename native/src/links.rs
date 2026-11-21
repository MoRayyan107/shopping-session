//! Stage 3: linked IDs (rules.md "Linked IDs"). Mirrors `pipeline/links.py::build_person_map`:
//! edges are non-NULL, non-self links; each connected component is named by its byte-wise smallest ID.
use crate::raw::Row;
use crate::util::FxMap;

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
