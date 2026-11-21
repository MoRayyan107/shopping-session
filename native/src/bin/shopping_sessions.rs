//! Native Shopping Sessions pipeline. Reads $DATA_DIR, writes Parquet to $OUTPUT_DIR, logs to stderr.
//! With $NATIVE_DUMP_DIR set, every stage also writes its tables there (same names as the oracle's dumps);
//! $NATIVE_DUMP_TABLES limits which.
use std::path::PathBuf;
use std::time::Instant;

use shopping_sessions_native::dump::Dumper;
use shopping_sessions_native::events::{Event, PURCHASE, TYPE_NAMES};
use shopping_sessions_native::pq::{write_table, Col, CAMPAIGN_DAILY, USER_DAILY};
use shopping_sessions_native::raw::{parse, prefetch, taken_all, Mapped};
use shopping_sessions_native::span::Store;
use shopping_sessions_native::ts::parse_ts;
use shopping_sessions_native::{clicks, daily, events, links, sessions};

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
    /// with NATIVE_PROFILE: (cpu seconds, minor faults, major faults) of the process at the last lap
    usage: Option<(f64, u64, u64)>,
}

/// (user + system cpu seconds, minor faults, major faults) of this process, from /proc/self/stat.
fn proc_usage() -> Option<(f64, u64, u64)> {
    let s = std::fs::read_to_string("/proc/self/stat").ok()?;
    let f: Vec<&str> = s[s.rfind(')')? + 2..].split(' ').collect();
    // after "pid (comm) ": state=0 ... minflt=7 majflt=9 utime=11 stime=12 (in clock ticks, 100 per second)
    let n = |i: usize| f.get(i).and_then(|v| v.parse::<u64>().ok());
    Some(((n(11)? + n(12)?) as f64 / 100.0, n(7)?, n(9)?))
}

impl Clock {
    fn lap(&mut self, what: &str, detail: impl std::fmt::Display) {
        let now = Instant::now();
        let wall = (now - self.last).as_secs_f64();
        let mut extra = String::new();
        if let (Some(prev), Some(cur)) = (self.usage, self.usage.and(proc_usage())) {
            let threads = rayon::current_num_threads() as f64;
            extra = format!(
                "   [cpu {:.2} s = {:.0}% of {} threads; faults {} minor, {} major]",
                cur.0 - prev.0,
                100.0 * (cur.0 - prev.0) / (wall * threads).max(1e-9),
                threads,
                cur.1 - prev.1,
                cur.2 - prev.2
            );
            self.usage = Some(cur);
        }
        eprintln!("[{wall:7.3}s] {what}: {detail}{extra}");
        self.last = now;
    }
}

const S: &str = "OPTIONAL BYTE_ARRAY";

fn main() {
    let data = env_path("DATA_DIR", "/data");
    let out = env_path("OUTPUT_DIR", "/output");
    if let Some(n) = std::env::var("NCPU").ok().and_then(|s| s.parse::<usize>().ok()) {
        rayon::ThreadPoolBuilder::new().num_threads(n).build_global().expect("thread pool");
    }
    let dump = Dumper::from_env();
    let profile = std::env::var("NATIVE_PROFILE").is_ok();
    let mut clock = Clock { start: Instant::now(), last: Instant::now(), usage: if profile { proc_usage() } else { None } };

    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("read manifest.json");
    let ws = manifest_value(&manifest, "window_start").and_then(|s| parse_ts(s.as_bytes())).expect("window_start");
    let we = manifest_value(&manifest, "window_end").and_then(|s| parse_ts(s.as_bytes())).expect("window_end");
    eprintln!("window [{ws}, {we}) ms");

    // Stage 1: parse
    // the mappings live until the process exits, so they are leaked as &'static (the prefetch thread borrows them)
    let ev_files: &'static Mapped = Box::leak(Box::new(Mapped::dir(&data.join("events")).expect("map events")));
    let ck_files: &'static Mapped = Box::leak(Box::new(Mapped::dir(&data.join("clicks")).expect("map clicks")));
    let ln_files: &'static Mapped = Box::leak(Box::new(Mapped::dir(&data.join("links")).expect("map links")));
    // read-ahead: the links first (they are parsed before anything else), the events and clicks once the
    // links are in, so the small links read never queues behind a gigabyte of event reads
    let pf_on = std::env::var("NATIVE_PREFETCH").as_deref() != Ok("0");
    let pf_step = std::env::var("NATIVE_PREFETCH_MB").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(4) << 20;
    let pf_threads = std::env::var("NATIVE_PREFETCH_THREADS").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(4);
    // How far the events + clicks read-ahead may run past the parse (0 = unbounded). Unbounded when MEM_GB
    // holds the whole input in the page cache beside the program's own memory (about 1.1x the input) with room
    // to spare (MEM_GB >= 2.5x the input); otherwise (or without MEM_GB) 512 MB, so under memory pressure the
    // read-ahead never evicts input pages still to be used. NATIVE_PREFETCH_AHEAD_MB overrides. Only a hint.
    let input_bytes = ev_files.bytes() + ck_files.bytes() + ln_files.bytes();
    let mem_bytes = std::env::var("MEM_GB").ok().and_then(|s| s.trim().parse::<f64>().ok()).map(|g| g * (1u64 << 30) as f64);
    let pf_ahead = match std::env::var("NATIVE_PREFETCH_AHEAD_MB").ok().and_then(|s| s.parse::<usize>().ok()) {
        Some(mb) => mb << 20,
        None if mem_bytes.is_some_and(|m| m >= 2.5 * input_bytes as f64) => 0,
        None => 512 << 20,
    };
    eprintln!("read-ahead window: {}", if pf_ahead == 0 { "unbounded".to_string() } else { format!("{} MB", pf_ahead >> 20) });
    if pf_on {
        prefetch(&[ln_files], pf_step, pf_threads, 0);
    }
    let pr_file = Mapped::file(&data.join("products.csv")).expect("map products.csv");
    let ca_file = Mapped::file(&data.join("campaigns.csv")).expect("map campaigns.csv");
    clock.lap("map files", format!("{:.0} MB", (ev_files.bytes() + ck_files.bytes() + ln_files.bytes()) as f64 / 1e6));
    // every string is an 8-byte Span into these files (or the side store)
    let (store, first) = Store::new(&[ev_files, ck_files, ln_files]);
    let links = parse::<2>(&ln_files);
    if pf_on {
        prefetch(&[ev_files, ck_files], pf_step, pf_threads, pf_ahead);
    }
    let products = parse::<2>(&pr_file);
    let campaigns = parse::<2>(&ca_file);
    clock.lap("parse links, products, campaigns", format!("{} / {} / {}", links.len(), products.len(), campaigns.len()));
    dump.raw("links", ["user_id_a", "user_id_b"], &links);
    // Stage 2 pass 1 (split + validate events) on the pool's threads, beside stage 3 (linked IDs) on a thread
    // of its own with a 1-thread pool: inside the main pool, the person map's parallel steps would queue
    // behind the split's workers and finish last.
    let cats = events::Categories::new(&products);
    let (ev_split, (pairs, ptab)) = std::thread::scope(|sc| {
        let linked = sc.spawn(|| {
            let mut t0 = std::time::Instant::now();
            let pool = rayon::ThreadPoolBuilder::new().num_threads(1).build().expect("person map pool");
            let pairs = pool.install(|| links::person_pairs(&links, &store));
            let ptab = links::PersonTable::new(&pairs);
            shopping_sessions_native::util::prof("links: person map (own thread, beside the split)", &mut t0);
            (pairs, ptab)
        });
        let ev_split = events::split(&ev_files, first[0], &store, &cats, ws, we);
        (ev_split, linked.join().expect("person map thread"))
    });
    dump.table("person_map", &format!("message person_map {{ {S} id (STRING); {S} person (STRING); }}"), || {
        vec![
            Col::OptStr(pairs.iter().map(|p| Some(p.0.to_vec(&store))).collect()),
            Col::OptStr(pairs.iter().map(|p| Some(p.1.to_vec(&store))).collect()),
        ]
    });
    clock.lap(
        "events split + validate | links, linked IDs",
        format!("{} lines | {} links, {} renamed IDs", ev_split.len(), links.len(), pairs.len()),
    );
    drop(pairs);
    let built = events::dedup(ev_split, &ptab, &store);
    clock.lap(
        "events dedup + person",
        format!("{} kept, {} valid", built.kept, built.by_person.iter().map(Vec::len).sum::<usize>()),
    );
    if dump.wants("raw_events") || dump.wants("raw_clicks") {
        dump.raw("raw_events", ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"], &parse::<7>(&ev_files));
        dump.raw("raw_clicks", ["click_id", "user_id", "campaign_id", "ts"], &parse::<4>(&ck_files));
    }
    let user_of = |e: &Event| e.row(&store)[1].unwrap().to_vec();
    let product_of = |e: &Event| e.row(&store)[5].unwrap().to_vec();
    let price_of = |e: &Event| (e.kind() == PURCHASE).then_some(e.price() as i64);
    dump.table(
        "ev_valid",
        &format!(
            "message ev_valid {{ {S} user_id (STRING); OPTIONAL INT64 ts_ms; {S} event_type (STRING);              {S} product_id (STRING); OPTIONAL INT64 price_cents; }}"
        ),
        || {
            let all = || built.by_person.iter().flatten();
            vec![
                Col::OptStr(all().map(|e| Some(user_of(e))).collect()),
                Col::OptI64(all().map(|e| Some(e.ts)).collect()),
                Col::OptStr(all().map(|e| Some(TYPE_NAMES[e.kind() as usize].to_vec())).collect()),
                Col::OptStr(all().map(|e| Some(product_of(e))).collect()),
                Col::OptI64(all().map(price_of).collect()),
            ]
        },
    );

    // Stage 4: sessions, day
    let mut parts = sessions::sessionize(built.by_person);
    clock.lap("sessions", parts.iter().map(|p| p.sessions.len()).sum::<usize>());
    if dump.on() {
        let all_s = || parts.iter().flat_map(|p| p.sessions.iter());
        // every event with its session (events of a session are contiguous)
        let all_e = || {
            parts.iter().flat_map(|p| {
                p.sessions.iter().flat_map(move |s| {
                    p.events[s.first as usize..(s.first + s.n_events) as usize].iter().map(move |e| (s, e))
                })
            })
        };
        dump.table(
            "ev_sessioned",
            &format!(
                "message ev_sessioned {{ {S} person (STRING); OPTIONAL INT64 ts_ms; OPTIONAL INT64 session_start; \
                 OPTIONAL INT32 day (DATE); {S} event_type (STRING); {S} product_id (STRING); \
                 OPTIONAL INT64 price_cents; {S} category (STRING); }}"
            ),
            || {
                vec![
                    Col::OptStr(all_e().map(|(_, e)| Some(e.person.to_vec(&store))).collect()),
                    Col::OptI64(all_e().map(|(_, e)| Some(e.ts)).collect()),
                    Col::OptI64(all_e().map(|(s, _)| Some(s.start)).collect()),
                    Col::OptDate(all_e().map(|(s, _)| Some(s.day)).collect()),
                    Col::OptStr(all_e().map(|(_, e)| Some(TYPE_NAMES[e.kind() as usize].to_vec())).collect()),
                    Col::OptStr(all_e().map(|(_, e)| Some(product_of(e))).collect()),
                    Col::OptI64(all_e().map(|(_, e)| price_of(e)).collect()),
                    Col::OptStr(all_e().map(|(_, e)| Some(cats.names[cats.of(e.row(&store)[5].unwrap()) as usize].to_vec())).collect()),
                ]
            },
        );
        dump.table(
            "sessions",
            &format!(
                "message sessions {{ {S} person (STRING); OPTIONAL INT64 session_start; OPTIONAL INT32 day (DATE); \
                 OPTIONAL INT64 n_events; OPTIONAL INT64 revenue_cents; OPTIONAL BOOLEAN has_purchase; }}"
            ),
            || {
                vec![
                    Col::OptStr(all_s().map(|s| Some(s.person.to_vec(&store))).collect()),
                    Col::OptI64(all_s().map(|s| Some(s.start)).collect()),
                    Col::OptDate(all_s().map(|s| Some(s.day)).collect()),
                    Col::OptI64(all_s().map(|s| Some(s.n_events as i64)).collect()),
                    Col::OptI64(all_s().map(|s| Some(s.revenue)).collect()),
                    Col::OptBool(all_s().map(|s| Some(s.has_purchase)).collect()),
                ]
            },
        );
    }

    // Stage 5: user_daily
    let ud = daily::user_daily(&parts, &cats);
    clock.lap("user_daily", ud.iter().map(Vec::len).sum::<usize>());
    dump.table("user_daily", USER_DAILY, || daily::user_daily_cols(&store, &ud));
    // nothing after user_daily reads the events (credit and assists use the sessions)
    for p in parts.iter_mut() {
        p.events = Vec::new();
    }

    // Stage 6: clicks, credit, assists
    let camps = clicks::Campaigns::new(&campaigns);
    let ck = clicks::build(&ck_files, first[1], &store, &ptab, &camps, ws, we);
    taken_all();
    eprintln!("long IDs (over the inline limit): {}", store.long_ids());
    drop(ptab);
    clock.lap("clicks dedup + validate + person", ck.iter().map(Vec::len).sum::<usize>());
    let cr = clicks::credit(&parts, &ck, &camps, dump.wants("session_assist"));
    clock.lap(
        "credit + assists",
        format!(
            "{} credited, {} assists",
            cr.iter().map(|c| c.credit.iter().filter(|&&x| x != camps.organic).count()).sum::<usize>(),
            cr.iter().map(|c| c.assist_counts.values().sum::<i64>()).sum::<i64>()
        ),
    );
    if dump.on() {
        dump.table(
            "ck_valid",
            &format!(
                "message ck_valid {{ {S} person (STRING); {S} click_id (STRING); {S} campaign_id (STRING); \
                 OPTIONAL INT64 ts_ms; }}"
            ),
            || {
                let all = || ck.iter().flatten();
                vec![
                    Col::OptStr(all().map(|c| Some(c.person.to_vec(&store))).collect()),
                    Col::OptStr(all().map(|c| Some(c.click_id.to_vec(&store))).collect()),
                    Col::OptStr(all().map(|c| Some(camps.names[c.camp as usize].to_vec())).collect()),
                    Col::OptI64(all().map(|c| Some(c.ts)).collect()),
                ]
            },
        );
        let sc = || parts.iter().zip(&cr).flat_map(|(p, c)| p.sessions.iter().zip(&c.credit));
        dump.table(
            "session_credit",
            &format!(
                "message session_credit {{ {S} person (STRING); OPTIONAL INT64 session_start; OPTIONAL INT32 day (DATE); \
                 OPTIONAL BOOLEAN has_purchase; OPTIONAL INT64 revenue_cents; {S} campaign_id (STRING); }}"
            ),
            || {
                vec![
                    Col::OptStr(sc().map(|(s, _)| Some(s.person.to_vec(&store))).collect()),
                    Col::OptI64(sc().map(|(s, _)| Some(s.start)).collect()),
                    Col::OptDate(sc().map(|(s, _)| Some(s.day)).collect()),
                    Col::OptBool(sc().map(|(s, _)| Some(s.has_purchase)).collect()),
                    Col::OptI64(sc().map(|(s, _)| Some(s.revenue)).collect()),
                    Col::OptStr(sc().map(|(_, c)| Some(camps.names[*c as usize].to_vec())).collect()),
                ]
            },
        );
        let sa = || {
            parts.iter().zip(&cr).flat_map(|(p, c)| c.assists.iter().map(move |(i, k)| (&p.sessions[*i as usize], *k)))
        };
        dump.table(
            "session_assist",
            &format!(
                "message session_assist {{ {S} person (STRING); OPTIONAL INT64 session_start; OPTIONAL INT32 day (DATE); \
                 {S} campaign_id (STRING); }}"
            ),
            || {
                vec![
                    Col::OptStr(sa().map(|(s, _)| Some(s.person.to_vec(&store))).collect()),
                    Col::OptI64(sa().map(|(s, _)| Some(s.start)).collect()),
                    Col::OptDate(sa().map(|(s, _)| Some(s.day)).collect()),
                    Col::OptStr(sa().map(|(_, k)| Some(camps.names[k as usize].to_vec())).collect()),
                ]
            },
        );
    }
    if dump.on() {
        clock.lap("stage dumps", "");
    }
    drop(ck);
    if store.side_len() > 0 {
        eprintln!("side store: {} values over the span limits", store.side_len());
    }

    // Stage 7: campaign_daily, self-checks, Parquet output
    let cd = daily::campaign_daily(&parts, &cr, &camps);
    clock.lap("campaign_daily", cd.len());
    let n_sessions: i64 = parts.iter().map(|p| p.sessions.len() as i64).sum();
    let ud_sessions: i64 = ud.iter().flatten().map(|r| r.sessions).sum();
    let cd_sessions: i64 = cd.iter().map(|r| r.sessions).sum();
    if ud_sessions != n_sessions || cd_sessions != n_sessions {
        eprintln!("WARNING self-check: sessions {n_sessions}, user_daily {ud_sessions}, campaign_daily {cd_sessions}");
    } else {
        eprintln!("self-checks: ok");
    }
    for t in ["user_daily", "campaign_daily"] {
        std::fs::create_dir_all(out.join(t)).expect("create output folder");
    }
    std::thread::scope(|s| {
        s.spawn(|| {
            daily::write_user_daily(&store, &out.join("user_daily"), &ud, rayon::current_num_threads()).expect("write user_daily")
        });
        s.spawn(|| {
            write_table(&out.join("campaign_daily/part-0.parquet"), CAMPAIGN_DAILY, daily::campaign_daily_cols(&cd))
                .expect("write campaign_daily")
        });
    });
    clock.lap("write parquet", format!("{} + {} rows", ud.iter().map(Vec::len).sum::<usize>(), cd.len()));

    eprintln!("[{:7.3}s] total", clock.start.elapsed().as_secs_f64());
    // The outputs are closed; skip freeing gigabytes of buffers one by one.
    std::process::exit(0);
}
