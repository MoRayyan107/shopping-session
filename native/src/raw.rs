//! Stage 1: parse every CSV of an input folder (or one file) into rows of borrowed byte slices.
//! Mirrors `pipeline/csv_parse.py::load_table`: malformed lines are dropped, empty values are `None`.
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use rayon::prelude::*;

use crate::csv::{chunks, lines, map_dir, map_file, split_line};

/// The memory-mapped files of one input; rows borrow from these.
pub struct Mapped {
    pub files: Vec<(PathBuf, Option<Mmap>)>,
}

impl Mapped {
    pub fn dir(dir: &Path) -> std::io::Result<Mapped> {
        if !dir.is_dir() {
            return Ok(Mapped { files: Vec::new() });
        }
        Ok(Mapped { files: map_dir(dir)? })
    }
    pub fn file(path: &Path) -> std::io::Result<Mapped> {
        Ok(Mapped { files: vec![(path.to_path_buf(), map_file(path)?)] })
    }
    pub fn bytes(&self) -> usize {
        self.files.iter().filter_map(|(_, m)| m.as_ref().map(|m| m.len())).sum()
    }
}

pub type Row<'a, const N: usize> = [Option<&'a [u8]>; N];

const CHUNK: usize = 4 << 20;

/// The data of every file cut into pieces of about 4 MB that end at a `\n` (headers skipped).
pub fn pieces(m: &Mapped) -> Vec<&[u8]> {
    m.files.iter().filter_map(|(_, f)| f.as_deref()).flat_map(|d| chunks(d, CHUNK)).collect()
}

/// Every well-formed data line of every file, in a deterministic order (file order, then line order).
pub fn parse<const N: usize>(m: &Mapped) -> Vec<Row<'_, N>> {
    let pieces = pieces(m);
    let parts: Vec<Vec<Row<'_, N>>> = pieces
        .par_iter()
        .map(|piece| {
            let mut out = Vec::with_capacity(piece.len() / 64);
            let mut f: Row<'_, N> = [None; N];
            for line in lines(piece) {
                if split_line(line, &mut f) {
                    out.push(f);
                }
            }
            out
        })
        .collect();
    let total = parts.iter().map(Vec::len).sum();
    let mut rows = Vec::with_capacity(total);
    for p in parts {
        rows.extend(p);
    }
    rows
}
