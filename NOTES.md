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

The unverified 8-byte line splitter is set aside as `git stash` "8-byte splitter (unverified)" in the D: repo, rescued as `wip-patches/02-stash-8byte-splitter.patch` on branch `speed-wip` (also
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

**Commit `dcc09e9a7e5c254b8d65cd3fc625b094c8d8c381` (tag `memory-v1`, branches `memory` and `main`).** Fresh-clone
proof passed: clone from GitHub, checkout, `--no-cache` build 29 s, 36/36 fixtures, dev MATCH (2.85 s), p20m MATCH
(2.51 s, anon 1.9 GB).

Earlier, with mallopt: p60m 12g exit 0, pipeline 13.1 s, anon peak 5.68 GB, `memory.peak`
10.67 GB (4.9 GB of it input cache), MATCH. 10g exit 0, 10.8 s, MATCH. **8g does not finish** (stopped at
400 s, 340k major faults): anon ~4.3 GB plus ~4.5 GB of input pages do not fit in 8 GiB, and dedup and sessions
read IDs from the input at random. The threshold is between 8 and 10 GiB; the scorer has 24 GB.

**mallopt: dropped** (removed from the code). Fixed 128 KB mmap/trim thresholds cost ~0.35 s at p20m on the new
layout (2.83-2.92 s with, 2.51-2.55 s without; `597cf7d` 2.70-2.77 s) and at p60m 12g gave 13.1 s with against
9.3 / 11.8 s without, for 0.2 GB less anon (5.68 vs 5.86 GB). The smaller layout is what fixed p60m.

Practice scores on the scorer: `597cf7d` 4.09 s, `dcc09e9` 3.88 s, both correct (leaders 0.75-0.94 s).

Optimisation history at 20M (in Docker, tmpfs output): first exact run 14.1 s; campaigns interned (credit +
assists 5.2 -> 0.5 s) 12.4 s; validate while parsing, id buckets then person buckets, person-hash sorting
3.2 s; parallel part files 2.8 s; packed-key dedup and the person bit filter 2.45 s.

Where the time goes at 20M now: split + validate 0.9-1.05 s (parse alone is ~0.5 s), dedup + person
0.5 s, sessions 0.35-0.4 s, clicks 0.37 s, credit 0.12 s, everything else ~0.2 s. Ideas not yet tried:
SWAR line/field splitting, smaller records (offsets instead of slices), a dictionary for `top_category`.

## Earlier plan

A design (for approval) for a native Rust pipeline. The practice data is about 20M events, the leaders are
at 1 to 1.5 s, and the final is about 60M events with a 3-minute cap.

## Speed work (branch `speed`)

Cold vs warm, p20m, `dcc09e9` (`tests/scorer_run.sh`, `COLD=1` drops the Docker VM page cache with
`echo 1 > /proc/sys/vm/drop_caches` in a privileged container): cold whole run 3.80-4.18 s, pipeline 3.36-3.68 s
(the scorer's 3.88 s is a cold number); warm 2.94-2.99 s / 2.48-2.52 s. About 1.0-1.2 s is waiting for the disk,
almost all in the events split (cold 1.7 s, warm 0.93 s). Raw cold read of p20m (1.83 GB): 1.06 s with one
reader, 0.57 s with 8 parallel `cat`s (3.2 GB/s): the I/O floor here. ~0.45 s per run is outside the pipeline
(container start/stop, wrapper).

`tests/ab.sh VOLUME RUNS BASE_IMAGE [MEM]`: alternating cold/warm runs of the base image's binary and the
candidate in `native-target`, scorer flags (through `tests/scorer_run.sh`). Baseline image:
`shopping-sessions:fresh-dcc09e9` (the fresh-clone build of the submitted commit).

### 1. Read-ahead (`raw::prefetch`, `util::map_in_order`): verified, committed

- `raw::prefetch`: madvise WILLNEED over the files in their order of use, 4 threads taking 4 MB chunks from a
  shared counter; links first, events + clicks once the links are parsed. Only a hint to the kernel: it never
  changes what is read. `NATIVE_PREFETCH=0` turns it off; `_MB`, `_THREADS`, `_MODE=read` (pread, no better).
- `util::map_in_order`: events/clicks pieces handed out strictly in order (results in piece order, exactly
  as `par_iter().map_init().collect()`), so the parse moves front to back behind the read-ahead.
- **Window.** Unbounded read-ahead hurt under memory pressure: p60m cold at 10g 23.4 s, 299k major faults
  (`dcc09e9` 20.1 s, 116k): input pages still to be read at random were evicted. So the events + clicks
  read-ahead runs at most 512 MB past what the parse has taken (`raw::TAKEN`), unless `MEM_GB >= 2.5x` the
  input (the program's own memory is ~1.1x the input, so the whole input then fits in the cache beside it):
  then unbounded. On the scorer (24 GB) p20m and p60m are both unbounded; locally p20m at 12g is unbounded,
  p60m at 12g / 10g windowed. `NATIVE_PREFETCH_AHEAD_MB` overrides (0 = unbounded).
- Results (all cold runs after the machine was kept awake, see below):

| Run | `dcc09e9` | read-ahead |
| --- | --- | --- |
| p20m cold, whole run / pipeline | 3.95-4.03 / 3.49-3.57 s | **3.37-3.47 / 2.90-2.96 s** |
| p20m warm, whole run / pipeline | 3.03-3.14 / 2.54-2.66 s | 3.00-3.05 / 2.54-2.60 s (same) |
| p60m 14g cold, unbounded (the scorer's mode) | 11.7 s | **9.5 s**, MATCH |
| p60m 12g cold, windowed | 11.4 s | 11.9 s (one earlier run 10.9 s), MATCH, anon 5.95 GB |
| p60m 10g cold, windowed | 19.6-20.1 s | 18.1-20.6 s, MATCH, anon 6.0 GB |

  Verified exact: 17 unit tests; every stage table + both outputs on all 61 datasets and on p20m.
  Parsing links inside the join branch was worse (the person map becomes the critical path); not kept.

### 2. Exact 16-byte ID keys (`key.rs`): verified, committed

**Where the time was** (warm p20m, `perf` in a privileged dev container, `tests` binary built with line tables
into the `native-target-prof` volume; CPU time per stage from `/proc/self/stat` with `NATIVE_PROFILE=1`): every
stage keeps the 8 threads 92-98% busy (CPU-bound, not waiting), 18.6 CPU-seconds in all. ~22% of all samples
were `memcmp` and ~5% `memcpy`: comparing user/person IDs by their bytes in the input files, long after those
lines left the cache. A person who is not renamed was named by the span of *their own event line*, so two events
of one person had different spans, and `same_person` compared bytes at two cold places for every adjacent pair
(sessions: 10.8% of all CPU), likewise `PersonTable::lookup` in dedup and clicks. The 8-byte line splitter
(stash) gave nothing measurable (split 0.973 vs 0.978 s): finding `
` / `,` / `"` is not where split's time goes.

**The change.** A user or person is now a `Key` (two u64, 16 bytes, align 8): an ID of at most 15 bytes inline
(bytes zero-padded big-endian, length in the lowest byte: equal keys <=> equal IDs, and keys order exactly as
the bytes do), a longer ID a *canonical* span (the first span registered for those bytes, `Store::key`, a
sharded mutex map; 0xFF in the lowest byte). So equal keys <=> equal IDs for every ID, and nothing reads the
input to compare IDs. The bucket hash is a function of the key. Record sizes unchanged (event line 48 B, event
40 B, click line 48 B, click 40 B, session 48 B, person slot 32 B): the 8-byte hash fields went. Sessions sort by
(person key, ts): no collision check or re-sort any more. `NATIVE_KEY_INLINE_MAX=N` lowers the inline limit
to push IDs through the long path (tested with 4: every 9-byte ID is long).

| Run | `8b7df52` | exact keys |
| --- | --- | --- |
| p20m cold, whole run / pipeline | 3.29-3.48 / 2.81-3.00 s | **2.85-3.09 / 2.36-2.55 s** |
| p20m warm, whole run / pipeline | 3.05-3.22 / 2.53-2.68 s | **2.40-2.58 / 1.95-2.09 s** |
| p60m 12g cold | 11.0 s | **9.7 s**, MATCH, anon 5.99 GB |
| p60m 10g cold | 18-20 s, ~100k major faults | **14.5 s**, 7.6k major faults, MATCH, anon 6.02 GB |

Warm p20m stages: split 0.85 s, dedup 0.55 s, sessions 0.35 -> 0.16 s, clicks 0.30 s, credit 0.13 -> 0.07 s.
Verified exact: 20 unit tests (incl. every ID up to 3 bytes over {0x00, a, b, 0xFF} for equality and order,
random IDs of every length 0-15, canonical long keys); every stage table + outputs on all 61 datasets and p20m;
the same with `NATIVE_KEY_INLINE_MAX=4` (61/61 + p20m); the side-store check `NATIVE_SPAN_MAX_LEN=4` (41/41).

### 3. Categories only for purchases; the person map on keys, on its own thread: verified, committed

- The split looked up every event's product in a hash map (13% of the split); only purchases need a category
  (`top_category`). Now only purchases look it up; the `ev_sessioned` dump recomputes every event's category
  from its line, so the dump is still a full check.
- With the split at ~0.78 s, the old person map (0.78 s, serial, byte-keyed hash map with `memcmp` into the
  links file) became the critical path. `links::person_pairs`: both keys of every link in parallel, interned by
  hash partition in parallel, union-find on integers, the name by key order (inline keys order as the bytes).
  Run inside the main pool beside the split its parallel steps queue behind the split's workers (0.86 s), and
  before the split it costs 0.16 s on the critical path, so it runs on a thread of its own with a 1-thread
  pool (~0.45 s there, hidden behind the split).

| p20m vs `fe3d063` (3 alternating runs) | `fe3d063` | this |
| --- | --- | --- |
| warm whole run / pipeline | 2.69-2.74 / 2.16-2.20 s | 2.49-2.67 / 2.01-2.14 s |
| cold whole run / pipeline | 3.01-3.08 / 2.52-2.55 s | 2.96-2.98 / 2.40-2.47 s |

(This batch ran while the machine was ~0.1 s slower than in the previous one; compare within a batch only.)
p60m cold 12g 9.6 s (base 9.0 s in the same batch; every stage, also untouched ones, 5-30% slower: noise),
10g 17.7 s; both MATCH, anon 6.0 GB. Verified exact: 22 unit tests (person map: chains, repeats both ways,
self links, empty sides, long and short IDs mixed); all stage tables + outputs on 61 datasets and p20m; the
same with `NATIVE_KEY_INLINE_MAX=4` (61/61); side store (41/41).

### 4. Dedup without reading lines back: verified, committed

- Events: the record keeps the event_id's exact key (not a hash) and ingest_ts in ms when it is an F1 string
  (20 bytes, ends in `Z`). F1 strings are fixed width, most significant first, so their byte order is their
  time order: when every copy of an id has an F1 ingest and the smallest is unique, that copy is the one the
  oracle keeps (ingest_ts as a string first). Otherwise (another form, or a tie on ingest) the copies' lines
  are split and ordered exactly as before. On dev every ingest is F1 and the 77 tied groups hold identical
  rows; the fallback keeps exactness for any data.
- Clicks: the record keeps the click_id's key; copies of one click_id whose parsed values (user, ts,
  campaign + validity) agree resolve to any one of them (they produce the same click); otherwise the lines are
  ordered as before. The click carries the key instead of a span, and the per-person order compares keys
  (inline keys order as bytes; long keys are read back), so no `span_of` and no cold line read per click.
- Cost: `Rec` 48 -> 64 B (events), click `Rec` 56 B, `Click` 40 -> 48 B: p60m anon peak 6.0 -> 7.3 GB.

| p20m vs `7167548` (3 alternating runs, all 6 pairs faster) | `7167548` | this |
| --- | --- | --- |
| warm whole run / pipeline | 2.43-2.54 / 1.90-2.01 s | 2.28-2.38 / 1.79-1.85 s |
| cold whole run / pipeline | 2.77-2.87 / 2.29-2.40 s | 2.62-2.87 / 2.14-2.36 s |
| events dedup / clicks dedup stage | 0.51 / 0.31 s | 0.40 / 0.24 s |

p60m cold: 12g 9.5 -> 8.4 s, 10g 14.7 -> 8.8 s (major faults 6.7k -> 1.2k: no lines re-read); both MATCH.
Verified exact: 24 unit tests (F1 parsing and order, click_id key order = byte order); all stage tables +
outputs on 61 datasets and p20m; the same with `NATIVE_KEY_INLINE_MAX=4` (61/61 and p20m); side store (41/41).

**Fresh-clone proof of `918544b55430749be0b6499250c6e2830626e6f7` (2026-11-21): passed.** Clone from GitHub,
checkout, clean tree, `--no-cache` build (`shopping-sessions:fresh-918544b`), 36/36 fixtures through
`run_fixtures.py` + `scripts/docker_run.sh` (TMPDIR on the host, MSYS_NO_PATHCONV=1), dev MATCH (pipeline 0.61 s),
p20m with the scorer's flags at 24g from `vol_p20m` (the host `data/p20m` folder is an empty placeholder):
cold 2.68 s whole run / 2.13 s pipeline, warm 2.40 / 1.80 s, anon 2.3-2.5 GB, MATCH both.

### Tried, not kept

- SSE2 tokenizer (64-byte newline/comma/quote masks; lines without quotes cut at the mask positions, others
  through `split_line`; equal to `lines` + `split_line` on 60k random pieces): split 0.82 -> 0.80 s, whole runs
  4 of 6 pairs faster, 2 slower. Not measurably faster: the split is dominated by validation (timestamps,
  keys, the record push), not by finding the commas. Patch kept outside the repo.

### Environment

- `D:` is a USB drive (WD P40) that Windows powers off when idle ("USB selective suspend"); each time, Docker
  Desktop loses its `D:` share until Docker is restarted. Work now happens in a clone on `C:`
  (`C:\Users\Mohammad Rayyan\Documents\shopping-session`), with the oracle cache and small datasets copied to
  `..\data` (p20m / p60m inputs stay in the Docker volumes `vol_p20m` / `vol_p60m`; `..\data\p20m` is an empty
  folder, the mount point `native_check.py` needs inside the read-only `/data`). `dev-answer/` copied too.
- **The laptop goes into Modern Standby when idle, even mid-run**: the Docker VM freezes, a run's wall time grows
  by the standby (one p60m run: 768 s wall, 20.9 s by its own clock) and host-side timeouts do not fire. Before
  timing, keep it awake (`SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)` in a background
  PowerShell loop; `ES_SYSTEM_REQUIRED` alone did not stop it, `| ES_DISPLAY_REQUIRED` added) and check
  `Kernel-Power` events 506/507 afterwards; a run that overlaps one shows wall time far over its pipeline time.

### Next

Warm p20m now: split 0.85 s (the person map, 0.78 s on one thread, runs beside it and takes one of its 8
threads), dedup 0.55 s, clicks 0.30 s, sessions 0.16 s, write 0.07 s, credit 0.07 s; ~0.45-0.5 s per run is
outside the pipeline (an empty container run with the same flags: 0.40-0.45 s). Next: dedup without reading
lines back (half of dedup's CPU re-splits the copies' cold lines: 7.3% of lines are copies at p20m).
