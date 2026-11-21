//! The rules.md CSV dialect on raw bytes, byte for byte the same rules as `pipeline/csv_parse.py`.
//!
//! Only `\n` ends a line (`\r` is an ordinary byte). The first line of every file is its header. Fields are
//! separated by every comma; a field is bare (no quote at all) or wrapped whole in double quotes. A line with
//! the wrong field count, a quote anywhere else, or an empty line is malformed. An empty value is `None`.

use std::fs::File;
use std::path::{Path, PathBuf};

use memmap2::Mmap;

/// Split one line into exactly `N` fields. Returns `false` when the line is malformed.
#[inline]
pub fn split_line<'a, const N: usize>(line: &'a [u8], out: &mut [Option<&'a [u8]>; N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let mut n = 0;
    for field in line.split(|&b| b == b',') {
        if n == N {
            return false;
        }
        let value = match memchr_quote(field) {
            None => field,
            Some(_) => {
                let l = field.len();
                if l >= 2 && field[0] == b'"' && field[l - 1] == b'"' && memchr_quote(&field[1..l - 1]).is_none() {
                    &field[1..l - 1]
                } else {
                    return false;
                }
            }
        };
        out[n] = if value.is_empty() { None } else { Some(value) };
        n += 1;
    }
    n == N
}

#[inline(always)]
fn memchr_quote(s: &[u8]) -> Option<usize> {
    s.iter().position(|&b| b == b'"')
}

/// Every `*.csv` file directly in `dir`, memory-mapped. An empty file maps to an empty slice.
pub fn map_dir(dir: &Path) -> std::io::Result<Vec<(PathBuf, Option<Mmap>)>> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "csv") && p.is_file())
        .collect();
    paths.sort();
    paths.into_iter().map(|p| map_file(&p).map(|m| (p, m))).collect()
}

pub fn map_file(path: &Path) -> std::io::Result<Option<Mmap>> {
    let f = File::open(path)?;
    if f.metadata()?.len() == 0 {
        return Ok(None);
    }
    // SAFETY: the input files are read-only for the whole run.
    Ok(Some(unsafe { Mmap::map(&f)? }))
}

/// Cut a file's data (everything after its header line) into pieces of about `target` bytes that end
/// at a `\n` (or the end of the file). Lines never straddle two pieces.
pub fn chunks(data: &[u8], target: usize) -> Vec<&[u8]> {
    let Some(header_end) = data.iter().position(|&b| b == b'\n') else {
        return Vec::new(); // only a header (or nothing)
    };
    let mut rest = &data[header_end + 1..];
    let mut out = Vec::new();
    while !rest.is_empty() {
        if rest.len() <= target {
            out.push(rest);
            break;
        }
        let cut = match rest[target..].iter().position(|&b| b == b'\n') {
            Some(p) => target + p + 1,
            None => rest.len(),
        };
        out.push(&rest[..cut]);
        rest = &rest[cut..];
    }
    out
}

/// The lines of a piece (split on `\n` only; empty lines are skipped, as they are malformed anyway).
#[inline]
pub fn lines(piece: &[u8]) -> impl Iterator<Item = &[u8]> {
    piece.split(|&b| b == b'\n').filter(|l| !l.is_empty())
}

/// Whether the AVX2 row splitter may be used: the CPU has AVX2 and NATIVE_SIMD is not `off` (which forces the
/// reference path everywhere, to check it on whole datasets).
pub fn simd_enabled() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        let forced_off = std::env::var("NATIVE_SIMD").is_ok_and(|v| v == "off");
        #[cfg(target_arch = "x86_64")]
        {
            !forced_off && std::arch::is_x86_feature_detected!("avx2")
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            let _ = forced_off;
            false
        }
    })
}

/// Calls `f(line, fields)` for every well-formed line of `piece`, in order: exactly the lines and fields of
/// `lines(piece)` + `split_line` (the reference path, used when AVX2 is missing or switched off).
#[inline(always)]
pub fn for_each_row<'a, const N: usize, F: FnMut(&'a [u8], &[Option<&'a [u8]>; N])>(piece: &'a [u8], f: F) {
    #[cfg(target_arch = "x86_64")]
    if simd_enabled() {
        // SAFETY: AVX2 is present (checked at run time above).
        unsafe { rows_avx2::<N, F>(piece, f) };
        return;
    }
    rows_reference::<N, F>(piece, f);
}

#[inline(always)]
pub fn rows_reference<'a, const N: usize, F: FnMut(&'a [u8], &[Option<&'a [u8]>; N])>(piece: &'a [u8], mut f: F) {
    let mut row = [None; N];
    for line in lines(piece) {
        if split_line(line, &mut row) {
            f(line, &row);
        }
    }
}

/// Bit k of each mask set when byte k of the 128 is `\n` / `,` / `"`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    use std::arch::x86_64::*;
    let (nl, cm, qt) = (_mm256_set1_epi8(b'\n' as i8), _mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'"' as i8));
    let (mut n, mut c, mut q) = (0u128, 0u128, 0u128);
    for k in 0..4 {
        let v = _mm256_loadu_si256(p.add(32 * k) as *const __m256i);
        n |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, nl)) as u32 as u128) << (32 * k);
        c |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, cm)) as u32 as u128) << (32 * k);
        q |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, qt)) as u32 as u128) << (32 * k);
    }
    (n, c, q)
}

/// The AVX2 path. Each line that starts at least 128 bytes before the end of the piece and is shorter than
/// 128 bytes is cut from three bitmasks (newline, comma, quote) of the 128 bytes at its start: the line ends
/// at the first newline; it needs exactly N - 1 commas; a field with quotes must have exactly two, its first
/// and last byte (the dialect: bare, or wrapped whole in quotes). Every other line goes through `split_line`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn rows_avx2<'a, const N: usize, F: FnMut(&'a [u8], &[Option<&'a [u8]>; N])>(piece: &'a [u8], mut f: F) {
    let len = piece.len();
    let mut p = 0usize;
    let mut row = [None; N];
    while p + 128 <= len {
        let (n, c, q) = masks128(piece.as_ptr().add(p));
        if n == 0 {
            // a line of 128 bytes or more
            let end = piece[p..].iter().position(|&b| b == b'\n').map_or(len, |i| p + i);
            let line = &piece[p..end];
            if split_line(line, &mut row) {
                f(line, &row);
            }
            p = end + 1;
            continue;
        }
        let l = n.trailing_zeros() as usize;
        let line = &piece[p..p + l];
        let next = p + l + 1;
        if l == 0 {
            p = next;
            continue;
        }
        // the newline's bit, 1 << l, found without a variable shift
        let nb = n & n.wrapping_neg();
        let lm = nb - 1;
        let mut c = c & lm;
        // quote bits not yet claimed by a field (fields are taken left to right)
        let mut q = q & lm;
        if c.count_ones() as usize != N - 1 {
            p = next;
            continue;
        }
        // field k is line[a..b]: `sb` is the bit of its first byte (1 << a), `eb` the bit of the comma or newline
        // that ends it (1 << b); both move field by field without variable shifts
        let mut a = 0usize;
        let mut sb = 1u128;
        let mut ok = true;
        for (k, slot) in row.iter_mut().enumerate() {
            let eb = if k + 1 < N {
                let eb = c & c.wrapping_neg();
                c ^= eb;
                eb
            } else {
                nb
            };
            let b = eb.trailing_zeros() as usize;
            // the field's quote bits: every unclaimed quote below its end
            let fq = q & (eb - 1);
            q ^= fq;
            let (s, e) = if fq == 0 {
                (a, b)
            } else if sb != eb >> 1 && fq == sb | (eb >> 1) {
                // at least two bytes (first != last), quoted exactly at the first and the last
                (a + 1, b - 1)
            } else {
                ok = false;
                break;
            };
            *slot = if s == e { None } else { Some(&line[s..e]) };
            a = b + 1;
            sb = eb << 1;
        }
        if ok {
            f(line, &row);
        }
        p = next;
    }
    // the last lines (fewer than 128 bytes left)
    if p < len {
        rows_reference::<N, F>(&piece[p..], f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dialect() {
        let mut o = [None; 3];
        assert!(split_line(b"x,\"y\",z\r", &mut o));
        assert_eq!(o, [Some(&b"x"[..]), Some(&b"y"[..]), Some(&b"z\r"[..])]);
        assert!(!split_line(b"1,2,\"3\"\r", &mut o));
        assert!(!split_line(b"a,b", &mut o));
        assert!(!split_line(b"a,b,c,d", &mut o));
        assert!(!split_line(b"\"a\"b,c,d", &mut o));
        assert!(split_line(b"\"\",  ,", &mut o));
        assert_eq!(o, [None, Some(&b"  "[..]), None]);
    }
    /// The AVX2 rows against the reference on random pieces (short and long lines, quotes, empty fields,
    /// lines near the 128-byte window and the piece end), for several field counts.
    #[test]
    fn rows_equal_reference() {
        #[cfg(target_arch = "x86_64")]
        {
            if !std::arch::is_x86_feature_detected!("avx2") {
                return;
            }
            fn check<const N: usize>(piece: &[u8]) {
                let mut fast: Vec<(&[u8], [Option<&[u8]>; N])> = Vec::new();
                unsafe { rows_avx2::<N, _>(piece, |l, r| fast.push((l, *r))) };
                let mut reference = Vec::new();
                rows_reference::<N, _>(piece, |l, r| reference.push((l, *r)));
                assert_eq!(fast, reference, "{:?}", String::from_utf8_lossy(piece));
            }
            let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
            let mut rnd = move || {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed
            };
            let alphabets: [&[u8]; 4] = [b"a,\"\r b\n,,", b"ab,,,\n", b"xyz,\"\n\r", b"abcdefghijklmnop,,\"\""];
            for round in 0..40_000 {
                let alphabet = alphabets[round % 4];
                let len = (rnd() % 700) as usize;
                let mut piece: Vec<u8> = (0..len).map(|_| alphabet[(rnd() % alphabet.len() as u64) as usize]).collect();
                // sometimes a newline every so often, so most lines are short
                if round % 3 == 0 {
                    let gap = 1 + (rnd() % 140) as usize;
                    for k in (0..piece.len()).step_by(gap) {
                        piece[k] = b'\n';
                    }
                }
                check::<1>(&piece);
                check::<2>(&piece);
                check::<4>(&piece);
                check::<7>(&piece);
            }
            // realistic lines: quoted fields, every length of an id around the 128-byte window
            for idlen in 0..140 {
                let id = "q".repeat(idlen);
                let one = format!("{id},u1,\"2027-12-20T22:03:47Z\",2027-12-20T22:04:52Z,\"purchase\",p4346,\"\"\n");
                for reps in [1usize, 2, 5] {
                    let piece = one.repeat(reps).into_bytes();
                    check::<7>(&piece);
                    let mut tail = piece.clone();
                    tail.extend_from_slice(b"a,b,c,d,e,f,g");
                    check::<7>(&tail);
                    check::<6>(&tail);
                }
            }
        }
    }

    #[test]
    fn chunking() {
        let d = b"h1,h2\na,b\nc,d\ne,f";
        let c = chunks(d, 3);
        assert_eq!(c.concat(), b"a,b\nc,d\ne,f".to_vec());
        assert!(c.iter().all(|p| p.ends_with(b"\n") || p.ends_with(b"f")));
        assert!(chunks(b"h1,h2\n", 3).is_empty());
        assert!(chunks(b"h1,h2", 3).is_empty());
    }
}
