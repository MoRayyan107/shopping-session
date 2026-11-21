//! Native Shopping Sessions pipeline. Reads $DATA_DIR, writes Parquet to $OUTPUT_DIR, logs to stderr.
//! With $NATIVE_DUMP_DIR set, every stage also writes its tables there (same names as the oracle's dumps).
use std::path::PathBuf;
use std::time::Instant;

use shopping_sessions_native::dump::Dumper;
use shopping_sessions_native::{events, links};
use shopping_sessions_native::pq::Col;
use shopping_sessions_native::raw::{parse, Mapped};
use shopping_sessions_native::ts::parse_ts;

fn env_path(key: &str, default: &str) -> PathBuf {
    PathBuf::from(std::env::var(key).unwrap_or_else(|_| default.to_string()))
}

/// The value of `"key": "..."` in manifest.json (a flat object of strings).
fn manifest_value(text: &str, key: &str) -> Option<String> {
    let at = text.find(&format!("\"{key}\""))? + key.len() + 2;
    let rest = &text[at..];
    let open = rest.find('"')? + 1;
    let close = rest[open..].find('"')? + open;
    Some(rest[open..close].to_string())
}

struct Clock {
    start: Instant,
    last: Instant,
}

impl Clock {
    fn lap(&mut self, what: &str, detail: impl std::fmt::Display) {
        let now = Instant::now();
        eprintln!("[{:7.3}s] {what}: {detail}", (now - self.last).as_secs_f64());
        self.last = now;
    }
}

fn main() {
    let data = env_path("DATA_DIR", "/data");
    let _out = env_path("OUTPUT_DIR", "/output");
    if let Some(n) = std::env::var("NCPU").ok().and_then(|s| s.parse::<usize>().ok()) {
        rayon::ThreadPoolBuilder::new().num_threads(n).build_global().expect("thread pool");
    }
    let dump = Dumper::from_env();
    let mut clock = Clock { start: Instant::now(), last: Instant::now() };

    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("read manifest.json");
    let ws = manifest_value(&manifest, "window_start").and_then(|s| parse_ts(s.as_bytes())).expect("window_start");
    let we = manifest_value(&manifest, "window_end").and_then(|s| parse_ts(s.as_bytes())).expect("window_end");
    eprintln!("window [{ws}, {we}) ms");

    // Stage 1: parse
    let ev_files = Mapped::dir(&data.join("events")).expect("map events");
    let ck_files = Mapped::dir(&data.join("clicks")).expect("map clicks");
    let ln_files = Mapped::dir(&data.join("links")).expect("map links");
    let pr_file = Mapped::file(&data.join("products.csv")).expect("map products.csv");
    let ca_file = Mapped::file(&data.join("campaigns.csv")).expect("map campaigns.csv");
    clock.lap("map files", format!("{:.0} MB", (ev_files.bytes() + ck_files.bytes() + ln_files.bytes()) as f64 / 1e6));
    let raw_events = parse::<7>(&ev_files);
    clock.lap("parse events", raw_events.len());
    let raw_clicks = parse::<4>(&ck_files);
    clock.lap("parse clicks", raw_clicks.len());
    let links = parse::<2>(&ln_files);
    let products = parse::<2>(&pr_file);
    let campaigns = parse::<2>(&ca_file);
    clock.lap("parse links, products, campaigns", format!("{} / {} / {}", links.len(), products.len(), campaigns.len()));
    dump.raw("raw_events", ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"], &raw_events);
    dump.raw("raw_clicks", ["click_id", "user_id", "campaign_id", "ts"], &raw_clicks);
    dump.raw("links", ["user_id_a", "user_id_b"], &links);
    if dump.on() {
        clock.lap("dump stage 1", "raw_events, raw_clicks, links");
    }

    // Stage 2: events dedup, validate, late filter
    let kept = events::dedup(&raw_events);
    clock.lap("events dedup", kept.len());
    let ev = events::validate(&raw_events, &kept, ws, we);
    clock.lap("events validate + late", ev.len());
    if dump.on() {
        dump.table(
            "ev_valid",
            "message ev_valid { OPTIONAL BYTE_ARRAY user_id (STRING); OPTIONAL INT64 ts_ms; \
             OPTIONAL BYTE_ARRAY event_type (STRING); OPTIONAL BYTE_ARRAY product_id (STRING); OPTIONAL INT64 price_cents; }",
            || vec![
                Col::OptStr(ev.iter().map(|e| Some(e.user.to_vec())).collect()),
                Col::OptI64(ev.iter().map(|e| Some(e.ts)).collect()),
                Col::OptStr(ev.iter().map(|e| Some(events::TYPE_NAMES[e.kind as usize].to_vec())).collect()),
                Col::OptStr(ev.iter().map(|e| Some(e.product.to_vec())).collect()),
                Col::OptI64(ev.iter().map(|e| (e.kind == events::PURCHASE).then_some(e.price)).collect()),
            ],
        );
        clock.lap("dump stage 2", "ev_valid");
    }

    // Stage 3: linked IDs
    let pmap = links::person_map(&links);
    clock.lap("linked IDs", format!("{} renamed IDs", pmap.len()));
    if dump.on() {
        dump.table(
            "person_map",
            "message person_map { OPTIONAL BYTE_ARRAY id (STRING); OPTIONAL BYTE_ARRAY person (STRING); }",
            || vec![
                Col::OptStr(pmap.keys().map(|k| Some(k.to_vec())).collect()),
                Col::OptStr(pmap.values().map(|v| Some(v.to_vec())).collect()),
            ],
        );
        clock.lap("dump stage 3", "person_map");
    }

    eprintln!("[{:7.3}s] total", clock.start.elapsed().as_secs_f64());
}
