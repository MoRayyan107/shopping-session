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

## Next

1. Quick wins in DuckDB, one at a time, each diffed against `oracle-v1` on the fixtures, dev, big3x and
   at least three more seeds (one with Feb 29, 2028): dedup on the raw `ingest_ts` string; `ts_parse`
   without regex extraction; CSV reading in parallel across file groups.
2. Then a design (for approval) for a native Rust pipeline. The practice data is about 20M events, the
   leaders are at 1 to 1.5 s, and the final is about 60M events with a 3-minute cap.
