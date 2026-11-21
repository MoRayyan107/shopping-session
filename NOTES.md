# Notes

## Status

The submission image is the native pipeline (`Dockerfile`, see below). `oracle-v1` is the full DuckDB pipeline. Both tables match all 36 fixtures and the 1M-event dev answer exactly
(`user_daily` 117,038 rows, `campaign_daily` 27,400 rows), both locally and in the scorer-style container.
It is the **correctness oracle**: every faster version must match it exactly on every seed.

## Stages (`pipeline/`)

Each stage is a function that reads and writes named DuckDB tables, so it can be swapped out and diffed.
Set `PIPELINE_DUMP_DIR` to also write every stage's tables to Parquet.

| # | Stage | File | Creates |
| - | ----- | ---- | ------- |
| 1 | CSV dialect: raw text, split on `\n` only, per-field quote check, `""`/empty -> NULL | `csv_parse.py` | `raw_events`, `raw_clicks`, `links`, `products`, `campaigns` |
| 2 | Strict timestamps F1/F2/F3 -> epoch ms or NULL | `ts_macros.sql` | macro `ts_parse` |
| 3 | Linked IDs: SQL edge cleanup, Python union-find on integers, byte-wise `min()` naming | `links.py` | `person_map` (only renamed IDs; use `LEFT JOIN` + `COALESCE`) |
| 4 | Events: dedup (smallest `ingest_ts`, before validation), validate, late filter, person, sessions on distinct timestamps, day, category | `events.py` | `ev_sessioned`, `sessions` |
| 5 | `user_daily` | `outputs.py` | `user_daily` |
| 6-7 | Clicks: dedup, validate, person; credit (one click per person and millisecond with the smallest `click_id`, then ASOF join); assists (range join) | `clicks.py` | `ck_valid`, `session_credit`, `session_assist` |
| 8 | `campaign_daily`, self-checks, Parquet | `outputs.py` | `campaign_daily` |

## Key decisions and findings

- **DuckDB's CSV reader is not used for the data.** It treats a bare `\r` as a line ending even with
  `new_line='\n'`, which would turn a bad price `1000\r` into a good one. Files are read with `read_text`
  (bytes kept exactly) and split on `\n` in SQL. A quote followed by `\r` (`"u1"\r`) is malformed.
- **DuckDB orders VARCHAR byte-wise** (UTF-8 bytes) for `ORDER BY`, `min`, `<`, `arg_min`: tested in
  `tests/test_byte_order.py` with upper-case, tab and non-ASCII IDs.
- **Timestamps** use a strict regex plus a hand-written calendar check, no lenient casts (a lenient Feb 30
  rolls into March and can land in the window). F3 values over 18 digits return NULL; they are outside
  every window anyway.
- **Never pass large Python lists as query parameters**: binding costs about 0.4 ms per element and
  grows faster than linearly (10,000 entries: 8 s). Union-find results go back through a temp CSV in scratch.
- **Equal timestamps share a session**: sessions are computed on distinct `(person, ts)` pairs and joined back.
- **Credit ties**: reduce clicks to one per (person, ms) with the smallest `click_id` first, then ASOF join.
- No Python touches events or clicks row by row; only the small links table goes through Python.

## How to run (Windows, this machine)

Python: only `py -3.14` works here (the 3.12/3.13 installs are broken). A venv with
`duckdb==1.5.6` and `polars` lives outside the repo. Below, `PY` is that venv's `python.exe`.

```sh
# unit tests
$PY -I tests/test_byte_order.py
$PY -I tests/test_timestamps.py
$PY -I tests/test_parse.py
$PY -I -u tests/test_links.py

# all 36 fixtures, row-level diffs, no Docker
$PY -I -u tests/fixtures_diff.py

# dev check: run, then the scorer's compare (no -I: compare.py imports its neighbour)
DATA_DIR=../data/dev OUTPUT_DIR=/some/out NCPU=8 MEM_GB=12 $PY -I pipeline/pipeline.py
$PY scripts/compare.py /some/out dev-answer

# the scorer-style container
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
TMPDIR=<a host folder> MSYS_NO_PATHCONV=1 CPUS=8 MEM=12g sh scripts/docker_run.sh ../data/dev /some/out
TMPDIR=<a host folder> MSYS_NO_PATHCONV=1 CPUS=8 MEM=12g $PY scripts/run_fixtures.py fixtures -- sh scripts/docker_run.sh
```

On Git Bash, `TMPDIR` must point at a host folder: otherwise `docker_run.sh`'s `mktemp -d` scratch is
created inside Docker's VM, owned by root, and uid 1000 cannot write to it. Docker Desktop here has
about 15 GiB, so local runs use `MEM=12g` rather than the scorer's 24 GB.

Data lives outside the repo: the generator (built in `rust:1.85.0-slim-bookworm`) is
`../tools/generate`, datasets are in `../data/` (`dev`: seed 1, 1M events; `big3x`: seed 7, 3M events).
`dev-answer/` and `generator/` are git-ignored.

## Timings (`oracle-v1`, local laptop, 8 threads; the scorer's will differ)

| Step | dev (1M) | big3x (3M) |
| ---- | -------- | ---------- |
| Timestamp parsing (dedup `ingest_ts` + validate `ts` + click `ts`) | 5.4 s | 12.3 s |
| CSV parsing, events + clicks | 1.5 s | 4.4 s |
| Everything else | < 1 s | ~2.5 s |
| **Total** | 7.9 s (6.9 s in Docker) | 19.2 s |

## Quick wins (after `oracle-v1`), each matching it exactly

Checked on the 36 fixtures and on `dev`, `big3x`, `s2`, `s4` and `s15` (seed 15's window holds Feb 29, 2028)
with `tests/check_oracle.py` (stored `oracle-v1` outputs in `../data/oracle/`). Best of 3, local.

| Change | dev total | big3x total |
| ------ | --------- | ----------- |
| `oracle-v1` | 7.87 s | 19.63 s |
| Dedup on the raw `ingest_ts` string (dedup 1.90 -> 0.27 s; the parse moved to validation) | 7.39 s | 18.57 s |
| `ts_parse` with `translate()` shape templates and fixed positions, no regex | 4.51 s | 12.04 s |
| CSV: materialise the lines so the dialect check runs on all threads (parse 4.05 -> 1.68 s on big3x) | **3.82 s** | **9.49 s** |
| Tried, not kept: read/split concurrently per file group (4 cursors). 1.71 -> 1.35 s in isolation, but no gain inside the pipeline | | |

Findings: DuckDB parallelises a query by chunks of its source rows, so work fed from `read_text` (one row
per file) runs on one thread; a list unnested straight out of `read_text` + `string_split` in a single query
showed quadratic cost in a probe, linear once materialised.

**At 20M events (practice size; seed 3, 1.8 GB) the DuckDB pipeline does not finish in 60 s**: parse events
14.0 s, dedup 6.2 s, validate 27.0 s, before sessions. Extrapolated about 70 s at 20M locally and over the
final's 3-minute cap at 60M. A native pipeline is needed, not just faster.

## Native pipeline (`native/`): the submission path

The top-level `Dockerfile` builds the native pipeline from the repository root (only files in git; the build
context is about 100 KB). The DuckDB pipeline stays as the fallback and correctness oracle: `Dockerfile.duckdb`
(`docker buildx build --platform linux/amd64 -f Dockerfile.duckdb .`) and the tag `oracle-v1`.

Rust 1.85 (`rust:1.85.0-slim-bookworm`, pinned by digest), crates locked: `memmap2 0.9.5`, `rayon 1.10.0`,
`parquet 59.3.3` (low-level writer, default features off; 59.3.3 is the newest supporting Rust 1.85).
The `Dockerfile` is multi-stage (runtime `debian:bookworm-slim` by digest, uid 1000, no Python, only the
`shopping_sessions` binary); a full no-cache build including crate downloads takes about 30 s.

How it works (`native/src/`):

1. `raw.rs`, `csv.rs`: memory-map every file, cut into ~4 MB pieces at `\n`, the exact rules.md dialect.
2. `events.rs` pass 1: one parallel pass splits **and validates** each line (validity depends only on the
   copy), writing a compact record into one of 256 buckets by hash(event_id). Linked IDs (`links.rs`,
   union-find, byte-wise smallest name) run on one thread meanwhile.
3. `events.rs` pass 2: per bucket, sort one packed u64 per row (40 hash bits + index); rows alone in their
   group are kept; groups of copies (or truncated-hash collisions) are re-split and resolved exactly by
   (event_id, the oracle's copy order). Kept valid events go to 256 buckets by hash(person), via
   `PersonTable` (precomputed hashes, a bit filter in front).
4. `sessions.rs`: sort each person bucket by (person hash, ts); if two persons ever share a 64-bit hash
   the bucket is re-sorted by the full person (unit-tested with forced collisions); 30-minute gap rule.
5. `daily.rs`: `user_daily` from contiguous (person, day) runs; top category by interned id, ties by name.
6. `clicks.rs`: the same split/dedup design, then credit and assists by one forward walk over each
   bucket's sessions and clicks (same person hash buckets); campaigns interned (organic = the key
   "organic", exactly as the oracle's string-keyed join).
7. `campaign_daily`, self-check, Parquet: `user_daily` as NCPU part files written in parallel (rules.md:
   file count does not matter), plain encoding, no compression.

**Oracle cache** (outside the repo, `../data/`): `oracle/<name>/` final tables and `oracle_dumps/<name>/` stage
dumps, built from a worktree pinned at the oracle commit by `tests/build_oracle.py`, plus `ev_valid`
(`tests/oracle_ev_valid.py`, the oracle's own `build_events`). Datasets: the 36 fixtures (`fx/<CASE>`),
`dev`, `big3x`, `s2`, `s4`, `s15`, 20 small seeds in `small/` (leap-day windows 62, 249 (Feb 29 is the last
day), 254, 272, 355; year-crossing 133, 188, 256, 419 (ends 2026-01-01); 100-110; 30k to 300k events),
`p20m` (seed 3, 20M events, 1.8 GB) and `p60m` (seed 5, 60M events, 15M clicks, 5.3 GB; final outputs only).

**Check**: `tests/native_check.py --tables T1,T2 [--outputs] ALL` runs the native binary in a linux/amd64
container and diffs each stage table against the oracle dump exactly (EXCEPT ALL both ways), and with
`--outputs` the final folders. `p20m` is read from the Docker volume `vol_p20m`. `NATIVE_DUMP_DIR` /
`NATIVE_DUMP_TABLES` turn on stage dumps; `NATIVE_PROFILE` logs sub-steps.

| Native stage | Tables | Result |
| ------------ | ------ | ------ |
| 1 Parse | `raw_events`, `raw_clicks`, `links` | match on all 61 datasets and on `p20m` |
| 2 Events dedup + validate + late | `ev_valid` | match on all 61 datasets and on `p20m` (19,649,172 rows) |
| 3 Linked IDs (union-find, byte-wise smallest name) | `person_map` | match on all 61 datasets and on `p20m` (812,055 rows) |
| 4 Person, sessions, day, category | `ev_sessioned`, `sessions` | match on all 61 datasets and on `p20m` (3,428,818 sessions) |
| 5 `user_daily` | `user_daily` | match on all 61 datasets and on `p20m` (2,255,788 rows) |
| 6 Clicks dedup + validate + person, credit, assists | `ck_valid`, `session_credit`, `session_assist` | match on all 61 datasets and on `p20m` (4,938,185 clicks; 37,582,598 assists) |
| 7 `campaign_daily`, self-checks, Parquet output | final `user_daily`, `campaign_daily` | match on all 61 datasets and on `p20m` (2,255,788 + 28,014 rows) |

All tables were re-checked after every optimisation below (the latest: every stage table and both outputs,
all 61 datasets, plus `p20m`).

**Scorer-style runs of the native image** (`scripts/docker_run.sh` flags: read-only root, 64 MB `/tmp`, no
network, caps dropped, uid 1000, 8 CPUs; memory 12g because Docker Desktop here has ~15 GiB):

- `scripts/run_fixtures.py`: 36/36 fixtures pass.
- dev: `scripts/compare.py` against `dev-answer`: MATCH. Pipeline 0.60-0.94 s warm (DuckDB: 3.8 s).
- p20m (data in a Docker volume, the closest to a local Linux disk): pipeline 2.58-2.84 s, whole
  `docker run` 3.1-3.4 s (DuckDB: about 70 s+).
- Data bind-mounted from the Windows host is slow on first read here (big3x: 10 s for 288 MB, then 1.4 s):
  Docker Desktop file sharing, not the pipeline. The scorer's disk will behave differently.

**Fresh-clone proof of `597cf7d` (2026-11-21): passed.** `git clone` from GitHub, checkout `597cf7d`, clean tree,
624 KB without `.git`; `docker buildx build --no-cache` 33 s; `run_fixtures.py fixtures -- sh scripts/docker_run.sh`
36/36; dev through `docker_run.sh` + `compare.py` against `dev-answer`: MATCH; p20m (`tests/scorer_run.sh`,
12g) + `compare.py` against `../data/oracle/p20m`: MATCH, exit 0. p20m cold: pipeline 4.25 s, wall 4.8 s; warm:
2.89 / 2.96 s, wall 3.44 s; program's own memory (anon) peak 4.2 GB.

**Running things on this machine (lessons):** the venv (Python 3.14, duckdb 1.5.6, polars 2.1.0) is
`C:/Users/MOHAMM~1/AppData/Local/Temp/claude/b72337224c9975d8/c98ad2ac-1c23-4bc4-af14-730a65c1af9b/scratchpad/venv/Scripts/python.exe`
(a temp folder: if it disappears, recreate it with `py -3.14 -m venv` + `pip install duckdb==1.5.6 polars`).
`TMPDIR` for `docker_run.sh` must be a folder outside Windows' Temp, e.g. `/d/Hackathon/JoinBuilders/dockertmp`:
Git Bash mounts Temp as `/tmp`, which Docker reads as a path in its own VM (root-owned, uid 1000 cannot write;
the run dies with "create output folder: Permission denied", reported as exit 139 because of panic=abort).
`tests/scorer_run.sh IMAGE VOLUME NAME [MEM]` is the record-keeping run for big datasets: scorer flags, no `--rm`,
output volume, exit code, wall time, anon peak sampled every 0.2 s, `memory.peak`, major faults, OOM events,
outputs copied to `../dockertmp/NAME/out`. Page cache is charged to the container that first read the file,
so `memory.peak` includes the input only on a cold run.

**p60m probe of `597cf7d` (2026-11-21, 15 GiB container): fails the final.** Split + validate 6.4 s,
dedup + person 27.6 s (0.5 s at 20M), then stuck in sessions; the container exited after 624 s (no OOM event;
the binary's own exit code and its outputs were lost: `--rm`, no output volume). At the sessions stage: anon
12.2 GB, page cache 3.4 GB, `memory.peak` 15.9 GB, 9.7M major page faults, 47.7 GB read from disk. Cause:
the program's own memory leaves too little room for the 5.6 GB of memory-mapped input, which later stages
read at random (person IDs are slices into the files), so pages are evicted and re-read over and over.
**glibc malloc settings on `597cf7d`** (environment only, `tests/scorer_run.sh`, 12g):

| Setting | p20m pipeline | p20m anon peak / memory.peak | p60m |
| ------- | ------------- | ----------------------------- | ---- |
| none | 2.65 / 2.73 s | 4.13 / 4.25 GB | still in dedup at 200 s (stopped), anon 11.1 GB |
| `MALLOC_ARENA_MAX=2` | 3.28 / 3.17 s (slower) | 4.11 / 4.22 GB | not run |
| arena max 2 + fixed mmap/trim thresholds 128 KB | 3.40 / 3.08 s | 3.5-3.6 / 3.77 GB | not run |
| `MALLOC_MMAP_THRESHOLD_=131072 MALLOC_TRIM_THRESHOLD_=131072` | 2.62 / 2.85 s | 3.05-3.75 / 3.77 GB | dedup 4.3 s (was 27.6 s), then stuck in sessions at 300 s (stopped), anon 10.2 GB, 249k major faults |

Useful but not enough: the record layout itself has to shrink. To be re-measured on top of the new layout and
kept only if it still helps.

The unverified 8-byte line splitter is set aside as `git stash` "8-byte splitter (unverified)" (also
`../dockertmp/splitter_8byte.patch`).

### Lower-memory layout (branch `memory`)

- `span.rs`: every string is an 8-byte `Span` (10 bits file, 34 bits offset, 20 bits length) into the mapped
  files. Over any limit (file index >= 1023, offset >= 16 GB, length >= 1 MB) the slice goes to a side store
  and the span holds its index, so every value resolves exactly. `NATIVE_SPAN_MAX_LEN=N` lowers the length
  limit to test the side store end to end.
- Records (checked by unit tests): event line 48 B (was 72), valid event 40 B (64), click line 48 B (80),
  valid click 40 B (56), session 48 B (56), person-table slot 32 B (40).
- No double copies: each ~4 MB piece writes its records into one array, then counting-sorts it by bucket
  (`util::sort_by_bucket`); a bucket is the run of slices over the pieces (`util::Buckets`), gathered into a
  per-thread scratch for dedup. Dedup output is sorted by person bucket per id bucket, the records are
  freed, then the person buckets are gathered once. The planned `unsafe` scatter was not needed: it gives
  the same peak (records + events are alive together either way), so there is no `unsafe` scatter.
- Freed once used: links rows after the person map, event records after dedup, events after `user_daily`,
  click records after the clicks are built, clicks after credit.

Verified exact: unit tests (span limits incl. 1,100 files and a value of MAX_LEN + 1 bytes, record sizes,
equal bytes with different spans, forced hash collisions); `tests/native_check.py` every stage table + both
outputs on all 61 datasets and on p20m; the same with `NATIVE_SPAN_MAX_LEN=4` on the 36 fixtures, dev and 4
small seeds (41/41; dev puts 2.96M values in the side store); p60m final outputs against the oracle.

**Final numbers (memory commit, no mallopt, `tests/scorer_run.sh`):** p60m 12g exit 0, pipeline 12.1 s, wall
12.9 s, anon peak 6.01 GB, `memory.peak` 10.94 GB (4.89 GB input cache), MATCH; p60m 10g exit 0, 8.8 s, anon
5.86 GB, MATCH (warm: the input cache was charged to the 12g run, so this is weaker evidence than a cold 10g
run). p20m: cold 3.70 s, warm 2.53 / 2.45 s (wall 2.98 / 2.86 s), anon 1.9 GB (`597cf7d`: 2.70-2.96 s, 4.2 GB).
Re-verified on this exact code: 17 unit tests, all 61 datasets and p20m on every stage table + outputs, the
side-store check (41/41).

Earlier, with mallopt: p60m 12g exit 0, pipeline 13.1 s, anon peak 5.68 GB, `memory.peak`
10.67 GB (4.9 GB of it input cache), MATCH. 10g exit 0, 10.8 s, MATCH. **8g does not finish** (stopped at
400 s, 340k major faults): anon ~4.3 GB plus ~4.5 GB of input pages do not fit in 8 GiB, and dedup and sessions
read IDs from the input at random. The threshold is between 8 and 10 GiB; the scorer has 24 GB.

**mallopt: dropped** (removed from the code). Fixed 128 KB mmap/trim thresholds cost ~0.35 s at p20m on the new
layout (2.83-2.92 s with, 2.51-2.55 s without; `597cf7d` 2.70-2.77 s) and at p60m 12g gave 13.1 s with against
9.3 / 11.8 s without, for 0.2 GB less anon (5.68 vs 5.86 GB). The smaller layout is what fixed p60m.

Practice score of `597cf7d` on the scorer: 4.09 s, correct (leaders 0.75-0.94 s).

Optimisation history at 20M (in Docker, tmpfs output): first exact run 14.1 s; campaigns interned (credit +
assists 5.2 -> 0.5 s) 12.4 s; validate while parsing, id buckets then person buckets, person-hash sorting
3.2 s; parallel part files 2.8 s; packed-key dedup and the person bit filter 2.45 s.

Where the time goes at 20M now: split + validate 0.9-1.05 s (parse alone is ~0.5 s), dedup + person
0.5 s, sessions 0.35-0.4 s, clicks 0.37 s, credit 0.12 s, everything else ~0.2 s. Ideas not yet tried:
SWAR line/field splitting, smaller records (offsets instead of slices), a dictionary for `top_category`.

## Earlier plan

A design (for approval) for a native Rust pipeline. The practice data is about 20M events, the leaders are
at 1 to 1.5 s, and the final is about 60M events with a 3-minute cap.
