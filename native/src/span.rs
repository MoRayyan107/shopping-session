//! 8-byte references to byte strings inside the memory-mapped input files, instead of 16-byte slices.
//!
//! A `Span` packs (file, offset, length) into one u64: 10 bits of file index, 34 bits of offset (16 GB per
//! file) and 20 bits of length (1 MB per value). Anything over a limit (file index >= 1023, offset >= 2^34,
//! length >= 2^20) goes to the side store: the slice itself is kept in a list and the span holds its index,
//! so every value resolves to exactly its bytes whatever the input. Equal spans always mean equal bytes; equal
//! bytes may have different spans, so comparisons of values compare the bytes (`Store::get`).
use std::sync::RwLock;

use crate::key::{Key, INLINE_MAX};
use crate::util::{hash_bytes, FxMap};

use crate::raw::Mapped;

const FILE_BITS: u32 = 10;
const OFF_BITS: u32 = 34;
const LEN_BITS: u32 = 20;
/// The file index that marks a side-store span (real files use 0 ..= SIDE - 1).
pub const SIDE: u64 = (1 << FILE_BITS) - 1;
pub const MAX_OFF: u64 = (1 << OFF_BITS) - 1;
pub const MAX_LEN: u64 = (1 << LEN_BITS) - 1;

#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct Span(u64);

impl Span {
    pub const EMPTY: Span = Span(0);
    #[inline(always)]
    fn file(self) -> u64 {
        self.0 >> (OFF_BITS + LEN_BITS)
    }
    #[inline(always)]
    fn off(self) -> u64 {
        (self.0 >> LEN_BITS) & MAX_OFF
    }
    #[inline(always)]
    fn len(self) -> u64 {
        self.0 & MAX_LEN
    }
    #[inline(always)]
    pub fn raw(self) -> u64 {
        self.0
    }
    #[inline(always)]
    pub fn from_raw(v: u64) -> Span {
        Span(v)
    }
}

const LONG_SHARDS: usize = 64;

/// (file, offset, length) as a span, or None when a part is over its limit (the value goes to the side
/// store). `max_len` is MAX_LEN except in the side-store tests (NATIVE_SPAN_MAX_LEN).
#[inline(always)]
pub fn pack(file: u64, off: u64, len: u64, max_len: u64) -> Option<Span> {
    if file >= SIDE || off > MAX_OFF || len > max_len {
        return None;
    }
    Some(Span((file << (OFF_BITS + LEN_BITS)) | (off << LEN_BITS) | len))
}

/// Every input byte string by file index, plus the side store.
pub struct Store<'a> {
    files: Vec<&'a [u8]>,
    /// (start address, end address, file index), sorted by address, for `span_of`
    by_addr: Vec<(usize, usize, u32)>,
    side: RwLock<Vec<&'a [u8]>>,
    max_len: u64,
    /// IDs up to this many bytes get inline keys (`key::INLINE_MAX`, lowered by NATIVE_KEY_INLINE_MAX to test
    /// the long path)
    inline_max: usize,
    /// canonical span of every long ID seen, sharded by hash
    long: Vec<std::sync::Mutex<FxMap<&'a [u8], Span>>>,
}

impl<'a> Store<'a> {
    /// All files of all inputs, numbered in order; `first(k)` is the index of input k's first file.
    pub fn new(inputs: &[&'a Mapped]) -> (Store<'a>, Vec<u32>) {
        let mut files = Vec::new();
        let mut first = Vec::new();
        for m in inputs {
            first.push(files.len() as u32);
            for (_, f) in &m.files {
                files.push(f.as_deref().unwrap_or(&[]));
            }
        }
        let max_len = std::env::var("NATIVE_SPAN_MAX_LEN").ok().and_then(|s| s.parse().ok()).unwrap_or(MAX_LEN).min(MAX_LEN);
        let inline_max =
            std::env::var("NATIVE_KEY_INLINE_MAX").ok().and_then(|s| s.parse().ok()).unwrap_or(INLINE_MAX).min(INLINE_MAX);
        (Store::from_slices_with_inline(files, max_len, inline_max), first)
    }

    pub fn from_slices(files: Vec<&'a [u8]>, max_len: u64) -> Store<'a> {
        Store::from_slices_with_inline(files, max_len, INLINE_MAX)
    }

    pub fn from_slices_with_inline(files: Vec<&'a [u8]>, max_len: u64, inline_max: usize) -> Store<'a> {
        let mut by_addr: Vec<(usize, usize, u32)> = files
            .iter()
            .enumerate()
            .filter(|(_, f)| !f.is_empty())
            .map(|(i, f)| (f.as_ptr() as usize, f.as_ptr() as usize + f.len(), i as u32))
            .collect();
        by_addr.sort_unstable();
        let long = (0..LONG_SHARDS).map(|_| std::sync::Mutex::new(FxMap::default())).collect();
        Store { files, by_addr, side: RwLock::new(Vec::new()), max_len, inline_max: inline_max.min(INLINE_MAX), long }
    }

    /// The exact key of ID `s`, which lies inside file `file`: inline when short, else its canonical span
    /// (the first span registered for these bytes).
    #[inline]
    pub fn key(&self, file: u32, s: &'a [u8]) -> Key {
        if s.len() <= self.inline_max {
            return Key::inline(s);
        }
        self.long_key(s, || self.span(file, s))
    }

    /// Like `key`, for a slice inside any of the files (found by address).
    pub fn key_of(&self, s: &'a [u8]) -> Key {
        if s.len() <= self.inline_max {
            return Key::inline(s);
        }
        self.long_key(s, || self.span_of(s))
    }

    #[cold]
    fn long_key(&self, s: &'a [u8], span: impl FnOnce() -> Span) -> Key {
        let shard = (hash_bytes(s) >> 58) as usize % LONG_SHARDS;
        let mut m = self.long[shard].lock().unwrap();
        Key::long(*m.entry(s).or_insert_with(span))
    }

    /// Distinct long IDs registered (logged).
    pub fn long_ids(&self) -> usize {
        self.long.iter().map(|m| m.lock().unwrap().len()).sum()
    }

    /// The span of `s`, which lies inside file `file`.
    #[inline]
    pub fn span(&self, file: u32, s: &'a [u8]) -> Span {
        if s.is_empty() {
            return Span::EMPTY;
        }
        let off = s.as_ptr() as usize - self.files[file as usize].as_ptr() as usize;
        match pack(file as u64, off as u64, s.len() as u64, self.max_len) {
            Some(sp) => sp,
            None => self.to_side(s),
        }
    }

    /// The span of `s`, which lies inside one of the files (found by address).
    pub fn span_of(&self, s: &'a [u8]) -> Span {
        if s.is_empty() {
            return Span::EMPTY;
        }
        let a = s.as_ptr() as usize;
        let i = self.by_addr.partition_point(|e| e.0 <= a);
        assert!(i > 0 && a + s.len() <= self.by_addr[i - 1].1, "span_of: slice outside the input files");
        self.span(self.by_addr[i - 1].2, s)
    }

    #[cold]
    fn to_side(&self, s: &'a [u8]) -> Span {
        let mut side = self.side.write().unwrap();
        side.push(s);
        let idx = (side.len() - 1) as u64;
        assert!(idx <= MAX_OFF, "side store full");
        // length 1 marks the span non-empty; the real length is the slice's
        Span((SIDE << (OFF_BITS + LEN_BITS)) | (idx << LEN_BITS) | 1)
    }

    #[inline(always)]
    pub fn get(&self, sp: Span) -> &'a [u8] {
        let len = sp.len() as usize;
        if len == 0 {
            return &[];
        }
        let f = sp.file();
        if f == SIDE {
            return self.side_get(sp.off() as usize);
        }
        let off = sp.off() as usize;
        &self.files[f as usize][off..off + len]
    }

    #[cold]
    fn side_get(&self, i: usize) -> &'a [u8] {
        self.side.read().unwrap()[i]
    }

    /// Values sent to the side store so far (logged, so a run that needed it is visible).
    pub fn side_len(&self) -> usize {
        self.side.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_limits() {
        // largest values that fit
        let sp = pack(SIDE - 1, MAX_OFF, MAX_LEN, MAX_LEN).unwrap();
        assert_eq!((sp.file(), sp.off(), sp.len()), (SIDE - 1, MAX_OFF, MAX_LEN));
        let sp = pack(0, 0, 1, MAX_LEN).unwrap();
        assert_eq!((sp.file(), sp.off(), sp.len()), (0, 0, 1));
        // one over any limit goes to the side store
        assert!(pack(SIDE, 0, 1, MAX_LEN).is_none());
        assert!(pack(0, MAX_OFF + 1, 1, MAX_LEN).is_none());
        assert!(pack(0, 0, MAX_LEN + 1, MAX_LEN).is_none());
        assert!(pack(0, 0, 9, 8).is_none());
        assert_eq!(std::mem::size_of::<Span>(), 8);
    }

    #[test]
    fn files_over_the_limit_and_long_values_resolve_exactly() {
        // 1,100 files: indices >= 1023 cannot be packed and go to the side store
        let data: Vec<Vec<u8>> = (0..1100).map(|i| format!("file{i},x").into_bytes()).collect();
        let store = Store::from_slices(data.iter().map(|v| v.as_slice()).collect(), MAX_LEN);
        for (i, d) in data.iter().enumerate() {
            let s = &d[4..d.len() - 2]; // the number
            let a = store.span(i as u32, s);
            let b = store.span_of(s);
            assert_eq!(store.get(a), s);
            assert_eq!(store.get(b), s);
            assert_eq!(a == b, i < SIDE as usize, "packed spans are unique per position");
        }
        assert_eq!(store.side_len(), 2 * (1100 - SIDE as usize));
        // a value of exactly MAX_LEN bytes packs; one byte more goes to the side store
        let big = vec![b'a'; MAX_LEN as usize + 1];
        let store = Store::from_slices(vec![&big], MAX_LEN);
        let fit = store.span(0, &big[1..]);
        let over = store.span(0, &big);
        assert_eq!(store.side_len(), 1);
        assert_eq!(store.get(fit).len(), MAX_LEN as usize);
        assert_eq!(store.get(over).len(), MAX_LEN as usize + 1);
        // empty values
        assert_eq!(store.get(store.span(0, &big[..0])), b"");
        assert_eq!(store.get(Span::EMPTY), b"");
    }

    #[test]
    fn lowered_max_len_sends_long_values_to_the_side_store() {
        let d = b"short,muchlongervalue".to_vec();
        let store = Store::from_slices(vec![&d], 8);
        let a = store.span(0, &d[..5]);
        let b = store.span(0, &d[6..]);
        assert_eq!((store.get(a), store.get(b)), (&b"short"[..], &b"muchlongervalue"[..]));
        assert_eq!(store.side_len(), 1);
    }
}
