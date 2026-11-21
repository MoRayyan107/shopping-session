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
