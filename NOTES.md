# Notes

## Status

`oracle-v1` is the full DuckDB pipeline. Both tables match all 36 fixtures and the 1M-event dev answer exactly
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

## Native pipeline (`native/`, in progress; NOT the submission path)

Rust 1.85 (`rust:1.85.0-slim-bookworm`, pinned by digest), crates locked: `memmap2 0.9.5`, `rayon 1.10.0`,
`parquet 59.3.3` (low-level writer, default features off; 59.3.3 is the newest supporting Rust 1.85).
`native/Dockerfile` is multi-stage (runtime `debian:bookworm-slim` by digest, uid 1000); a no-cache build
takes about 26 s. The top-level `Dockerfile` (DuckDB) stays the submission image until the native pipeline
matches everywhere and is faster.

- Parser benchmark (`bench_parse`) at 20M events (1,659 MB), 8 threads, warm cache: 0.50-0.56 s, and it finds
  exactly the oracle's 21,559,089 well-formed lines.
- Parquet output checked with polars (Date), DuckDB (DATE; INT32 DateType) and the scorer's loader.

**Oracle cache** (outside the repo, `../data/`): `oracle/<name>/` final tables and `oracle_dumps/<name>/` stage
dumps, built from a worktree pinned at the oracle commit by `tests/build_oracle.py`, plus `ev_valid`
(`tests/oracle_ev_valid.py`, the oracle's own `build_events`). Datasets: the 36 fixtures (`fx/<CASE>`),
`dev`, `big3x`, `s2`, `s4`, `s15`, 20 small seeds in `small/` (leap-day windows 62, 249 (Feb 29 is the last
day), 254, 272, 355; year-crossing 133, 188, 256, 419 (ends 2026-01-01); 100-110; 30k to 300k events), and
`p20m` (seed 3, 20M events, 1.8 GB; the oracle took 184 s with dumps).

**Check**: `tests/native_check.py --tables T1,T2 [--outputs] ALL` runs the native binary in a linux/amd64
container and diffs each stage table against the oracle dump exactly (EXCEPT ALL both ways). `p20m` is read
from the Docker volume `vol_p20m`.

| Native stage | Tables | Result |
| ------------ | ------ | ------ |
| 1 Parse | `raw_events`, `raw_clicks`, `links` | match on all 61 datasets and on `p20m` |
| 2 Events dedup + validate + late | `ev_valid` | match on all 61 datasets and on `p20m` (19,649,172 rows) |
| 3 Linked IDs (union-find, byte-wise smallest name) | `person_map` | match on all 61 datasets and on `p20m` (812,055 rows) |
| 4 Person, sessions, day, category | `ev_sessioned`, `sessions` | match on all 61 datasets and on `p20m` (3,428,818 sessions) |
| 5 `user_daily` | `user_daily` | match on all 61 datasets and on `p20m` (2,255,788 rows) |
| 6 Clicks dedup + validate + person, credit, assists | `ck_valid`, `session_credit`, `session_assist` | match on all 61 datasets and on `p20m` (4,938,185 clicks; 37,582,598 assists) |
| 7 `campaign_daily`, self-checks, Parquet output | final `user_daily`, `campaign_daily` | match on all 61 datasets and on `p20m` (2,255,788 + 28,014 rows) |

Native timings at 20M (in Docker, 8 threads, dumps excluded): parse events 1.31 s, clicks 0.20 s, events
dedup 0.92 s (hash-partitioned, 256 parts), validate + late 0.38-0.67 s, linked IDs 0.46 s (one thread), sessions 2.02 s, user_daily 0.06 s, clicks 0.50 s, credit + assists 4.89 s (materialises 37.6M assist rows), campaign_daily 0.32 s, write
Parquet 2.17 s. **First full native run at 20M: 14.1 s** (all stages exact; optimisation next). `NATIVE_DUMP_TABLES` limits which dumps are
written. To optimise once everything matches.

## Next

Native stages 2-7: dedup + validate + late, linked IDs, sessions, `user_daily`, clicks + credit + assists,
`campaign_daily` + Parquet output, each diffed against the oracle before it is committed.

## Earlier plan

A design (for approval) for a native Rust pipeline. The practice data is about 20M events, the leaders are
at 1 to 1.5 s, and the final is about 60M events with a 3-minute cap.
