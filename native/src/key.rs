//! 16-byte exact keys for IDs (user IDs and the persons they map to), so comparing two IDs never reads the
//! input files again.
//!
//! An ID of at most 15 bytes is stored inline: its bytes, zero-padded, in the top 15 bytes (big-endian) and
//! its length in the lowest byte. Two inline keys are equal exactly when the IDs are equal, and they order
//! exactly as the IDs do byte-wise (zero padding plus the length orders a prefix first).
//! A longer ID is a *canonical* span: the first span registered for those bytes (`Store::key`), with 0xFF in
//! the lowest byte, so equal long IDs also have equal keys. Equal keys <=> equal IDs, for every ID.
//! Long keys order by span (any fixed order is fine: only grouping needs it, never the output).
use crate::span::{Span, Store};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug)]
pub struct Key {
    hi: u64,
    lo: u64,
}

pub const INLINE_MAX: usize = 15;
const LONG: u64 = 0xFF;

#[inline(always)]
fn be_prefix(b: &[u8]) -> u64 {
    // the first min(len, 8) bytes, big-endian, zero-padded on the right
    if b.len() >= 8 {
        return u64::from_be_bytes(b[..8].try_into().unwrap());
    }
    let mut v = 0u64;
    for (i, &x) in b.iter().enumerate() {
        v |= (x as u64) << (56 - 8 * i);
    }
    v
}

impl Key {
    /// The inline key of an ID of at most INLINE_MAX bytes.
    #[inline(always)]
    pub fn inline(b: &[u8]) -> Key {
        let n = b.len();
        debug_assert!(n <= INLINE_MAX);
        let hi = be_prefix(b);
        let lo = if n > 8 {
            // bytes 8..n: the last n - 8 bytes of the (overlapping) final 8-byte word, moved to the top
            let tail = u64::from_be_bytes(b[n - 8..n].try_into().unwrap());
            tail << (8 * (16 - n))
        } else {
            0
        };
        Key { hi, lo: lo | n as u64 }
    }

    /// The key of a long ID, from its canonical span (only `Store::key` calls this).
    #[inline]
    pub fn long(canonical: Span) -> Key {
        Key { hi: canonical.raw(), lo: LONG }
    }

    #[inline(always)]
    pub fn is_inline(self) -> bool {
        self.lo & 0xFF != LONG
    }

    /// A well-mixed 64-bit hash (the top bits pick the bucket), a function of the ID's bytes only.
    #[inline(always)]
    pub fn hash(self) -> u64 {
        let mut x = self.hi ^ self.lo.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(29);
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
        x ^= x >> 33;
        x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        x ^= x >> 33;
        x
    }

    /// The ID's bytes.
    pub fn to_vec(self, store: &Store<'_>) -> Vec<u8> {
        if self.is_inline() {
            let n = (self.lo & 0xFF) as usize;
            let mut buf = [0u8; 16];
            buf[..8].copy_from_slice(&self.hi.to_be_bytes());
            buf[8..].copy_from_slice(&self.lo.to_be_bytes());
            buf[..n].to_vec()
        } else {
            store.get(Span::from_raw(self.hi)).to_vec()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_short_ids(alphabet: &[u8], max: usize) -> Vec<Vec<u8>> {
        let mut out = vec![Vec::new()];
        let mut layer = vec![Vec::new()];
        for _ in 0..max {
            let mut next = Vec::new();
            for p in &layer {
                for &c in alphabet {
                    let mut q: Vec<u8> = p.clone();
                    q.push(c);
                    next.push(q);
                }
            }
            out.extend(next.iter().cloned());
            layer = next;
        }
        out
    }

    #[test]
    fn inline_keys_are_exact_and_byte_ordered() {
        // every ID up to 3 bytes over an alphabet with 0x00, 0xFF and ordinary bytes
        let ids = all_short_ids(&[0x00, b'a', b'b', 0xFF], 3);
        let store = Store::from_slices(vec![], crate::span::MAX_LEN);
        for a in &ids {
            let ka = Key::inline(a);
            assert_eq!(ka.to_vec(&store), *a);
            assert!(ka.is_inline());
            for b in &ids {
                let kb = Key::inline(b);
                assert_eq!(ka == kb, a == b, "{a:?} {b:?}");
                assert_eq!(ka.cmp(&kb), a.cmp(b), "{a:?} {b:?}");
            }
        }
    }

    #[test]
    fn inline_keys_of_every_length() {
        // lengths 0..=15 with random bytes, including prefixes of each other and the 8-byte boundary
        let mut seed: u64 = 0x2545_F491_4F6C_DD1D;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let store = Store::from_slices(vec![], crate::span::MAX_LEN);
        let mut ids: Vec<Vec<u8>> = Vec::new();
        for _ in 0..3000 {
            let n = (rnd() % 16) as usize;
            let v: Vec<u8> = (0..n).map(|_| [0u8, 1, b'x', b'y', 0xFE, 0xFF][(rnd() % 6) as usize]).collect();
            ids.push(v);
        }
        let base: Vec<u8> = (0..15).map(|i| b'a' + i as u8).collect();
        for n in 0..=15 {
            ids.push(base[..n].to_vec());
        }
        for a in &ids {
            assert_eq!(Key::inline(a).to_vec(&store), *a);
        }
        for a in &ids {
            for b in ids.iter().take(400) {
                let (ka, kb) = (Key::inline(a), Key::inline(b));
                assert_eq!(ka == kb, a == b);
                assert_eq!(ka.cmp(&kb), a.cmp(b), "{a:?} {b:?}");
                if a == b {
                    assert_eq!(ka.hash(), kb.hash());
                }
            }
        }
    }

    #[test]
    fn long_keys_are_canonical() {
        // the same 20-byte ID in two places, another one, and a 15-byte one; with the inline limit lowered
        // to 4, the 9-byte IDs take the long path too
        let d = b"abcdefghijklmnopqrst,abcdefghijklmnopqrst,abcdefghijklmnopqrsu,abcdefghijklmno,u12345678,u12345678".to_vec();
        for limit in [INLINE_MAX, 4] {
            let store = Store::from_slices_with_inline(vec![&d], crate::span::MAX_LEN, limit);
            let f: Vec<&[u8]> = d.split(|&b| b == b',').collect();
            let k: Vec<Key> = f.iter().map(|s| store.key(0, s)).collect();
            assert_eq!(k[0], k[1], "equal long IDs at different places get one key");
            assert_ne!(k[0], k[2]);
            assert_ne!(k[0], k[3]);
            assert_eq!(k[4], k[5]);
            assert_eq!(k[0].hash(), k[1].hash());
            assert!(!k[0].is_inline());
            assert_eq!(k[3].is_inline(), limit == INLINE_MAX);
            assert_eq!(k[4].is_inline(), limit == INLINE_MAX);
            for (s, key) in f.iter().zip(&k) {
                assert_eq!(key.to_vec(&store), s.to_vec());
            }
            // a long key never equals an inline one, even of the same bytes
            if limit < f[3].len() {
                assert_ne!(store.key(0, f[3]), Key::inline(f[3]));
            }
            assert_eq!(store.key_of(f[1]), k[0]);
        }
    }

    #[test]
    fn key_size() {
        assert_eq!(std::mem::size_of::<Key>(), 16);
        assert_eq!(std::mem::align_of::<Key>(), 8);
    }
}
