//! 8-byte exact keys for IDs (user IDs and the persons they map to, event and click IDs), so comparing two
//! IDs never reads the input files again.
//!
//! A *compact* key holds an ID of at most COMPACT_MAX (10) bytes, all in `0-9A-Za-z`: each byte becomes a
//! 6-bit code (1..=62, increasing with the byte value), packed most significant first into bits 59..0 and
//! padded on the right with code 0. Two compact keys are equal exactly when the IDs are equal, and they order
//! exactly as the IDs do byte-wise (the padding code is below every byte's code, so a prefix orders first).
//! Bit 63 is clear.
//! Any other ID gets a *long* key: bit 63 set and the ID's index in the store's registry of long IDs, given
//! to the first occurrence of those bytes (`Store::key`), so equal long IDs also have equal keys.
//! Whether an ID is compact depends only on its bytes, so equal keys <=> equal IDs, for every ID.
//! Long keys order by registry index (any fixed order is fine: only grouping needs it, never the output;
//! byte order of mixed keys is read back, see `is_inline`).
use crate::span::Store;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug)]
pub struct Key(u64);

/// The longest ID a compact key holds.
pub const INLINE_MAX: usize = 10;
const LONG: u64 = 1 << 63;
/// Marks a byte that has no compact code (any byte outside `0-9A-Za-z`).
const BAD: u8 = 0xFF;

/// The 6-bit code of every byte: `0-9` -> 1..=10, `A-Z` -> 11..=36, `a-z` -> 37..=62, others BAD.
const CODE: [u8; 256] = {
    let mut t = [BAD; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = 1 + i as u8;
        i += 1;
    }
    let mut i = 0;
    while i < 26 {
        t[b'A' as usize + i] = 11 + i as u8;
        t[b'a' as usize + i] = 37 + i as u8;
        i += 1;
    }
    t
};

/// The byte of every code (index 0, the padding, is never decoded).
const BYTE: [u8; 64] = {
    let mut t = [0u8; 64];
    let mut c = 0;
    while c < 256 {
        if CODE[c] != BAD {
            t[CODE[c] as usize] = c as u8;
        }
        c += 1;
    }
    t
};

impl Key {
    /// The compact key of `b`, or None when it is longer than `max` (at most INLINE_MAX) or holds a byte
    /// outside `0-9A-Za-z`.
    #[inline(always)]
    pub fn compact(b: &[u8], max: usize) -> Option<Key> {
        debug_assert!(max <= INLINE_MAX);
        if b.len() > max {
            return None;
        }
        let mut v = 0u64;
        let mut bad = 0u8;
        for &c in b {
            let k = CODE[c as usize];
            bad |= k;
            v = (v << 6) | (k & 63) as u64;
        }
        // valid codes are below 64, so bit 7 is set only by a BAD byte
        if bad & 0x80 != 0 {
            return None;
        }
        Some(Key(v << (6 * (INLINE_MAX - b.len()))))
    }

    /// The compact key of an ID known to be compact (tests and constants).
    pub fn inline(b: &[u8]) -> Key {
        Key::compact(b, INLINE_MAX).expect("a compact ID")
    }

    /// The key of the long ID with registry index `idx` (only `Store::key` calls this).
    #[inline]
    pub fn long(idx: u64) -> Key {
        debug_assert!(idx < LONG);
        Key(LONG | idx)
    }

    /// The registry index of a long key.
    #[inline]
    pub fn long_index(self) -> u64 {
        self.0 & !LONG
    }

    /// The key's bits (orders exactly as the key).
    #[inline(always)]
    pub fn raw(self) -> u64 {
        self.0
    }

    /// True for a compact key (compact keys order exactly as their IDs' bytes).
    #[inline(always)]
    pub fn is_inline(self) -> bool {
        self.0 & LONG == 0
    }

    /// A well-mixed 64-bit hash (the top bits pick the bucket), a function of the key only.
    #[inline(always)]
    pub fn hash(self) -> u64 {
        let mut x = self.0 ^ 0x9E37_79B9_7F4A_7C15;
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
        x ^= x >> 33;
        x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        x ^= x >> 33;
        x
    }

    /// The ID's bytes.
    pub fn to_vec(self, store: &Store<'_>) -> Vec<u8> {
        let mut out = Vec::with_capacity(INLINE_MAX);
        self.append_to(store, &mut out);
        out
    }

    /// The ID's bytes, appended to `out`.
    #[inline]
    pub fn append_to(self, store: &Store<'_>, out: &mut Vec<u8>) {
        if self.is_inline() {
            for i in 0..INLINE_MAX {
                let c = (self.0 >> (6 * (INLINE_MAX - 1 - i))) & 63;
                if c == 0 {
                    break;
                }
                out.push(BYTE[c as usize]);
            }
        } else {
            out.extend_from_slice(store.long_bytes(self.long_index()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_ids(alphabet: &[u8], max: usize) -> Vec<Vec<u8>> {
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
    fn codes_increase_with_the_byte() {
        let valid: Vec<u8> = (0..=255u8).filter(|&c| CODE[c as usize] != BAD).collect();
        assert_eq!(valid.len(), 62);
        assert!(valid.windows(2).all(|w| CODE[w[0] as usize] < CODE[w[1] as usize]));
        assert!(valid.iter().all(|&c| c.is_ascii_alphanumeric() && BYTE[CODE[c as usize] as usize] == c));
        assert_eq!(CODE[b'0' as usize], 1);
        assert_eq!(CODE[b'z' as usize], 62);
    }

    #[test]
    fn compact_keys_are_exact_and_byte_ordered() {
        // every ID up to 3 bytes over the alphabet's edges and middle
        let ids = all_ids(b"09AMZamz", 3);
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
        assert_eq!(Key::inline(b""), Key::default());
    }

    #[test]
    fn compact_keys_of_every_length_and_the_limits() {
        // lengths 0..=10 with random alphanumeric bytes, including prefixes of each other
        let mut seed: u64 = 0x2545_F491_4F6C_DD1D;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let alpha = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        let store = Store::from_slices(vec![], crate::span::MAX_LEN);
        let mut ids: Vec<Vec<u8>> = Vec::new();
        for _ in 0..4000 {
            let n = (rnd() % 11) as usize;
            // a small alphabet slice makes equal and prefix IDs likely
            let lo = (rnd() % 60) as usize;
            ids.push((0..n).map(|_| alpha[lo + (rnd() % 3) as usize]).collect());
        }
        for n in 0..=10 {
            ids.push(b"zzzzzzzzzz"[..n].to_vec());
            ids.push(b"0000000000"[..n].to_vec());
        }
        for a in &ids {
            assert_eq!(Key::inline(a).to_vec(&store), *a);
        }
        for a in &ids {
            for b in ids.iter().take(500) {
                let (ka, kb) = (Key::inline(a), Key::inline(b));
                assert_eq!(ka == kb, a == b);
                assert_eq!(ka.cmp(&kb), a.cmp(b), "{a:?} {b:?}");
                if a == b {
                    assert_eq!(ka.hash(), kb.hash());
                }
            }
        }
        // the limits: 11 bytes, or any byte outside 0-9A-Za-z, is not compact; a lower max is honoured
        assert!(Key::compact(b"abcdefghijk", INLINE_MAX).is_none());
        for c in 0..=255u8 {
            let id = [b'u', c, b'1'];
            assert_eq!(Key::compact(&id, INLINE_MAX).is_some(), c.is_ascii_alphanumeric(), "byte {c}");
        }
        assert!(Key::compact(b"abcde", 4).is_none());
        assert!(Key::compact(b"abcd", 4).is_some());
        // the largest compact key leaves the long bit clear
        assert!(Key::inline(b"zzzzzzzzzz").is_inline());
    }

    #[test]
    fn long_keys_are_canonical() {
        // the same 20-byte ID in two places, another one, a 15-byte one, IDs with bytes outside the alphabet;
        // with the limit lowered to 4, the 9-byte IDs take the long path too
        let d = b"abcdefghijklmnopqrst,abcdefghijklmnopqrst,abcdefghijklmnopqrsu,abcdefghijklmno,u12345678,u12345678,u_1,u_1,u\xff,u\x00"
            .to_vec();
        for limit in [INLINE_MAX, 4] {
            let store = Store::from_slices_with_inline(vec![&d], crate::span::MAX_LEN, limit);
            let f: Vec<&[u8]> = d.split(|&b| b == b',').collect();
            let k: Vec<Key> = f.iter().map(|s| store.key(0, s)).collect();
            assert_eq!(k[0], k[1], "equal long IDs at different places get one key");
            assert_ne!(k[0], k[2]);
            assert_ne!(k[0], k[3]);
            assert_eq!(k[4], k[5]);
            assert_eq!(k[6], k[7]);
            assert_ne!(k[6], k[8]);
            assert_ne!(k[8], k[9]);
            assert_eq!(k[0].hash(), k[1].hash());
            assert!(!k[0].is_inline());
            assert!(!k[3].is_inline());
            assert!(!k[6].is_inline() && !k[8].is_inline() && !k[9].is_inline());
            assert_eq!(k[4].is_inline(), limit == INLINE_MAX);
            for (s, key) in f.iter().zip(&k) {
                assert_eq!(key.to_vec(&store), s.to_vec());
            }
            // a long key never equals a compact one
            if limit < f[4].len() {
                assert_ne!(store.key(0, f[4]), Key::inline(f[4]));
            }
            assert_eq!(store.key_of(f[1]), k[0]);
            assert_eq!(store.key_of(f[7]), k[6]);
            // every pair: equal keys <=> equal bytes
            for (a, ka) in f.iter().zip(&k) {
                for (b, kb) in f.iter().zip(&k) {
                    assert_eq!(ka == kb, a == b, "{a:?} {b:?}");
                }
            }
        }
    }

    #[test]
    fn key_size() {
        assert_eq!(std::mem::size_of::<Key>(), 8);
        assert_eq!(std::mem::align_of::<Key>(), 8);
    }
}
