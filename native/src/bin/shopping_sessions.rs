//! Native Shopping Sessions pipeline. Reads $DATA_DIR, writes Parquet to $OUTPUT_DIR, logs to stderr.
//! With $NATIVE_DUMP_DIR set, every stage also writes its tables there (same names as the oracle's dumps);
//! $NATIVE_DUMP_TABLES limits which.
use std::path::PathBuf;
use std::time::Instant;

use shopping_sessions_native::dump::Dumper;
use shopping_sessions_native::events::{Event, PURCHASE, TYPE_NAMES};
use shopping_sessions_native::pq::{write_table, Col, CAMPAIGN_DAILY, USER_DAILY};
use shopping_sessions_native::raw::{parse, Mapped};
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
}

impl Clock {
    fn lap(&mut self, what: &str, detail: impl std::fmt::Display) {
        let now = Instant::now();
        eprintln!("[{:7.3}s] {what}: {detail}", (now - self.last).as_secs_f64());
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
    // every string is an 8-byte Span into these files (or the side store)
    let (store, first) = Store::new(&[&ev_files, &ck_files, &ln_files]);
    let links = parse::<2>(&ln_files);
    let products = parse::<2>(&pr_file);
    let campaigns = parse::<2>(&ca_file);
    clock.lap("parse links, products, campaigns", format!("{} / {} / {}", links.len(), products.len(), campaigns.len()));
    dump.raw("links", ["user_id_a", "user_id_b"], &links);
    // Stage 2 pass 1 (split + validate events) runs while one thread does stage 3 (linked IDs)
    let cats = events::Categories::new(&products);
    let (ev_split, (n_renamed, ptab)) = rayon::join(
        || events::split(&ev_files, first[0], &store, &cats, ws, we),
        || {
            let m = links::person_map(&links);
            let t = links::PersonTable::new(&m, &store);
            dump.table("person_map", &format!("message person_map {{ {S} id (STRING); {S} person (STRING); }}"), || {
                vec![
                    Col::OptStr(m.keys().map(|k| Some(k.to_vec())).collect()),
                    Col::OptStr(m.values().map(|v| Some(v.to_vec())).collect()),
                ]
            });
            (m.len(), t)
        },
    );
    drop(links);
    clock.lap("events split + validate | linked IDs", format!("{} lines | {} renamed IDs", ev_split.len(), n_renamed));
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
    let mut parts = sessions::sessionize(&store, built.by_person);
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
                    Col::OptStr(all_e().map(|(_, e)| Some(store.get(e.person).to_vec())).collect()),
                    Col::OptI64(all_e().map(|(_, e)| Some(e.ts)).collect()),
                    Col::OptI64(all_e().map(|(s, _)| Some(s.start)).collect()),
                    Col::OptDate(all_e().map(|(s, _)| Some(s.day)).collect()),
                    Col::OptStr(all_e().map(|(_, e)| Some(TYPE_NAMES[e.kind() as usize].to_vec())).collect()),
                    Col::OptStr(all_e().map(|(_, e)| Some(product_of(e))).collect()),
                    Col::OptI64(all_e().map(|(_, e)| price_of(e)).collect()),
                    Col::OptStr(all_e().map(|(_, e)| Some(cats.names[e.cat as usize].to_vec())).collect()),
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
                    Col::OptStr(all_s().map(|s| Some(store.get(s.person).to_vec())).collect()),
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
    let ud = daily::user_daily(&store, &parts, &cats);
    clock.lap("user_daily", ud.iter().map(Vec::len).sum::<usize>());
    dump.table("user_daily", USER_DAILY, || daily::user_daily_cols(&store, &ud));
    // nothing after user_daily reads the events (credit and assists use the sessions)
    for p in parts.iter_mut() {
        p.events = Vec::new();
    }

    // Stage 6: clicks, credit, assists
    let camps = clicks::Campaigns::new(&campaigns);
    let ck = clicks::build(&ck_files, first[1], &store, &ptab, &camps, ws, we);
    drop(ptab);
    clock.lap("clicks dedup + validate + person", ck.iter().map(Vec::len).sum::<usize>());
    let cr = clicks::credit(&store, &parts, &ck, &camps, dump.wants("session_assist"));
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
                    Col::OptStr(all().map(|c| Some(store.get(c.person).to_vec())).collect()),
                    Col::OptStr(all().map(|c| Some(store.get(c.click_id).to_vec())).collect()),
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
                    Col::OptStr(sc().map(|(s, _)| Some(store.get(s.person).to_vec())).collect()),
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
                    Col::OptStr(sa().map(|(s, _)| Some(store.get(s.person).to_vec())).collect()),
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
