//! Parser benchmark: memory-map every events/*.csv, cut into chunks at `\n`, and on N threads run the
//! dialect check, both timestamp parses, the event-type check and the price check in one pass.
//!   bench_parse DATA_DIR [THREADS] [CHUNK_MB] [RUNS]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use shopping_sessions_native::csv::{chunks, lines, map_dir, split_line};
use shopping_sessions_native::ts::{parse_price, parse_ts};

#[derive(Default, Clone, Copy)]
struct Counts {
    lines: u64,
    well_formed: u64,
    with_event_id: u64,
    ts_ok: u64,
    ingest_ok: u64,
    type_ok: u64,
    price_ok: u64,
    checksum: i64,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data = std::path::PathBuf::from(&args[1]);
    let threads: usize = args.get(2).map_or(8, |s| s.parse().unwrap());
    let chunk_mb: usize = args.get(3).map_or(8, |s| s.parse().unwrap());
    let runs: usize = args.get(4).map_or(3, |s| s.parse().unwrap());

    for run in 0..runs {
        let t0 = Instant::now();
        let files = map_dir(&data.join("events")).expect("map events");
        let pieces: Vec<&[u8]> = files
            .iter()
            .filter_map(|(_, m)| m.as_deref())
            .flat_map(|d| chunks(d, chunk_mb << 20))
            .collect();
        let bytes: usize = files.iter().filter_map(|(_, m)| m.as_ref().map(|m| m.len())).sum();
        let t_map = t0.elapsed();

        let next = AtomicUsize::new(0);
        let t1 = Instant::now();
        let totals: Vec<Counts> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..threads)
                .map(|_| {
                    s.spawn(|| {
                        let mut c = Counts::default();
                        let mut f: [Option<&[u8]>; 7] = [None; 7];
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            if i >= pieces.len() {
                                break;
                            }
                            for line in lines(pieces[i]) {
                                c.lines += 1;
                                if !split_line(line, &mut f) {
                                    continue;
                                }
                                c.well_formed += 1;
                                if f[0].is_none() {
                                    continue;
                                }
                                c.with_event_id += 1;
                                if let Some(t) = f[2].and_then(parse_ts) {
                                    c.ts_ok += 1;
                                    c.checksum = c.checksum.wrapping_add(t);
                                }
                                if let Some(t) = f[3].and_then(parse_ts) {
                                    c.ingest_ok += 1;
                                    c.checksum = c.checksum.wrapping_add(t);
                                }
                                let ty = f[4];
                                if matches!(ty, Some(b"view") | Some(b"cart") | Some(b"purchase")) {
                                    c.type_ok += 1;
                                }
                                if ty == Some(b"purchase") && f[6].and_then(parse_price).is_some() {
                                    c.price_ok += 1;
                                }
                            }
                        }
                        c
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let t_parse = t1.elapsed();
        let mut t = Counts::default();
        for c in totals {
            t.lines += c.lines;
            t.well_formed += c.well_formed;
            t.with_event_id += c.with_event_id;
            t.ts_ok += c.ts_ok;
            t.ingest_ok += c.ingest_ok;
            t.type_ok += c.type_ok;
            t.price_ok += c.price_ok;
            t.checksum = t.checksum.wrapping_add(c.checksum);
        }
        eprintln!(
            "run {run}: {} files {:.0} MB, {} chunks, threads {threads}: map+chunk {:.3}s, parse {:.3}s ({:.0} MB/s), total {:.3}s",
            files.len(), bytes as f64 / 1e6, pieces.len(), t_map.as_secs_f64(), t_parse.as_secs_f64(),
            bytes as f64 / 1e6 / t_parse.as_secs_f64(), t0.elapsed().as_secs_f64()
        );
        eprintln!(
            "       lines {} well-formed {} with event_id {} ts ok {} ingest ok {} type ok {} purchase price ok {} checksum {}",
            t.lines, t.well_formed, t.with_event_id, t.ts_ok, t.ingest_ok, t.type_ok, t.price_ok, t.checksum
        );
    }
}
