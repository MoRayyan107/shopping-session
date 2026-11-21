//! Stage 1: parse every CSV of an input folder (or one file) into rows of borrowed byte slices.
//! Mirrors `pipeline/csv_parse.py::load_table`: malformed lines are dropped, empty values are `None`.
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use rayon::prelude::*;

use crate::csv::{chunks, map_dir, map_file};

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

/// Ask the kernel to start reading these inputs now (madvise WILLNEED), in order, `step` bytes per call, so
/// the disk works while the parse threads run. Only a hint: it never changes what is read.
/// `threads` threads take the next chunk in order from a shared counter, so up to `threads` requests are in
/// flight while the order stays the order of use.
/// `ahead` > 0 bounds the read-ahead: a chunk is requested only once it starts at most `ahead` bytes past
/// what the parse has taken (`note_taken`), so under memory pressure the read-ahead never evicts pages that
/// are still to be used (an unbounded one cost p60m at 10g 3 s and 2.5x the major faults). 0 = unbounded.
pub fn prefetch(inputs: &[&'static Mapped], step: usize, threads: usize, ahead: usize) {
    #[cfg(unix)]
    {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        // (mapping, offset, length, position of the chunk in the order of use)
        let mut chunks: Vec<(&'static Mmap, usize, usize, usize)> = Vec::new();
        let mut pos = 0;
        for m in inputs {
            for f in m.files.iter().filter_map(|(_, f)| f.as_ref()) {
                let mut off = 0;
                while off < f.len() {
                    let len = step.min(f.len() - off);
                    chunks.push((f, off, len, pos));
                    off += len;
                    pos += len;
                }
            }
        }
        let chunks = Arc::new(chunks);
        let next = Arc::new(AtomicUsize::new(0));
        // NATIVE_PREFETCH_MODE=read: read each chunk with pread into a scratch buffer (it lands in the page
        // cache, which the mapping shares) instead of madvise
        let read = std::env::var("NATIVE_PREFETCH_MODE").as_deref() == Ok("read");
        let paths: Arc<Vec<(usize, std::path::PathBuf)>> = Arc::new(
            inputs
                .iter()
                .flat_map(|m| m.files.iter().filter_map(|(p, f)| f.as_ref().map(|f| (f.as_ptr() as usize, p.clone()))))
                .collect(),
        );
        for _ in 0..threads.max(1) {
            let (chunks, next, paths) = (chunks.clone(), next.clone(), paths.clone());
            std::thread::spawn(move || {
                use std::os::unix::fs::FileExt;
                let mut buf = if read { vec![0u8; step] } else { Vec::new() };
                let mut open: Option<(usize, std::fs::File)> = None;
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(&(f, off, len, pos)) = chunks.get(i) else { break };
                    while ahead > 0 && pos > TAKEN.load(Ordering::Relaxed).saturating_add(ahead) {
                        std::thread::sleep(std::time::Duration::from_micros(200));
                    }
                    if !read {
                        let _ = f.advise_range(memmap2::Advice::WillNeed, off, len);
                        continue;
                    }
                    let key = f.as_ptr() as usize;
                    if open.as_ref().is_none_or(|o| o.0 != key) {
                        let Some((_, p)) = paths.iter().find(|(k, _)| *k == key) else { continue };
                        match std::fs::File::open(p) {
                            Ok(file) => open = Some((key, file)),
                            Err(_) => continue,
                        }
                    }
                    let _ = open.as_ref().unwrap().1.read_at(&mut buf[..len], off as u64);
                }
            });
        }
    }
}

/// Bytes of the events + clicks inputs the parse has taken so far (the bounded read-ahead follows it).
pub static TAKEN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// The parse took a piece of `n` bytes (events, then clicks, in the order `prefetch` was given them).
#[inline]
pub fn note_taken(n: usize) {
    TAKEN.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}

/// Parsing is over: release any read-ahead thread still waiting for its window.
pub fn taken_all() {
    TAKEN.store(usize::MAX / 2, std::sync::atomic::Ordering::Relaxed);
}

pub type Row<'a, const N: usize> = [Option<&'a [u8]>; N];

const CHUNK: usize = 4 << 20;

/// The data of every file cut into pieces of about 4 MB that end at a `\n` (headers skipped).
pub fn pieces(m: &Mapped) -> Vec<&[u8]> {
    m.files.iter().filter_map(|(_, f)| f.as_deref()).flat_map(|d| chunks(d, CHUNK)).collect()
}

/// Like `pieces`, with the Store index of each piece's file (`first` is the index of this input's first file).
pub fn pieces_with_file(m: &Mapped, first: u32) -> Vec<(u32, &[u8])> {
    m.files
        .iter()
        .enumerate()
        .filter_map(|(i, (_, f))| f.as_deref().map(|d| (first + i as u32, d)))
        .flat_map(|(i, d)| chunks(d, CHUNK).into_iter().map(move |p| (i, p)))
        .collect()
}

/// Every well-formed data line of every file, in a deterministic order (file order, then line order).
pub fn parse<const N: usize>(m: &Mapped) -> Vec<Row<'_, N>> {
    let pieces = pieces(m);
    let parts: Vec<Vec<Row<'_, N>>> = pieces
        .par_iter()
        .map(|piece| {
            let mut out = Vec::with_capacity(piece.len() / 64);
            crate::csv::for_each_row::<N, _>(piece, |_, f| out.push(*f));
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
