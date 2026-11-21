#!/usr/bin/env python3
"""
Shopping Sessions pipeline (DuckDB). This version is the correctness oracle: every stage is a function
that reads and writes named DuckDB tables, so any stage can be replaced by a faster implementation and
diffed against this one.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)
Debug  $PIPELINE_DUMP_DIR: if set, each stage's tables are also written there as Parquet.
All logging goes to stderr.
"""
import json
import os
import sys
import tempfile
import time

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from csv_parse import load_table  # noqa: E402
from events import build_events  # noqa: E402
from links import build_person_map  # noqa: E402
from clicks import build_clicks  # noqa: E402
from outputs import (CAMPAIGN_DAILY_COLUMNS, USER_DAILY_COLUMNS, build_campaign_daily,  # noqa: E402
                     build_user_daily, self_checks, write_parquet)

EVENTS = ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]
CLICKS = ["click_id", "user_id", "campaign_id", "ts"]
LINKS = ["user_id_a", "user_id_b"]


def log(*args):
    print(*args, file=sys.stderr, flush=True)


def main():
    data = os.environ.get("DATA_DIR", "/data")
    out = os.environ.get("OUTPUT_DIR", "/output")
    scratch = os.environ.get("SCRATCH_DIR", "/scratch")
    if not os.path.isdir(scratch):                       # local runs without a /scratch
        scratch = tempfile.mkdtemp(prefix="pipeline_scratch_")
    dump_dir = os.environ.get("PIPELINE_DUMP_DIR")
    ncpu = int(os.environ.get("NCPU", os.cpu_count() or 1))
    mem_gb = int(os.environ.get("MEM_GB", "8"))

    con = duckdb.connect()
    con.execute(f"SET threads = {ncpu}")
    con.execute(f"SET memory_limit = '{max(1, int(mem_gb * 0.8))}GB'")
    con.execute(f"SET temp_directory = '{os.path.join(scratch, 'duckdb_tmp').replace(chr(92), '/')}'")
    con.execute("SET preserve_insertion_order = false")
    with open(os.path.join(HERE, "ts_macros.sql"), encoding="utf-8") as f:
        con.execute(f.read())

    timings = []
    t_all = time.time()

    def stage(name, fn, dump_tables=()):
        t = time.time()
        result = fn()
        took = time.time() - t
        timings.append((name, took))
        log(f"[{took:7.2f}s] {name}: {result}")
        if dump_dir:
            os.makedirs(dump_dir, exist_ok=True)
            for table in dump_tables:
                con.execute(f"COPY {table} TO '{os.path.join(dump_dir, table + '.parquet').replace(chr(92), '/')}' (FORMAT parquet)")
        return result

    with open(os.path.join(data, "manifest.json"), encoding="utf-8") as f:
        manifest = json.load(f)
    ws, we = (con.execute("SELECT ts_parse(?)", [manifest[k]]).fetchone()[0] for k in ("window_start", "window_end"))
    if ws is None or we is None:
        raise SystemExit(f"cannot parse the manifest window: {manifest}")
    log(f"window {manifest['window_start']} .. {manifest['window_end']} = [{ws}, {we}) ms")

    j = lambda *p: os.path.join(data, *p)  # noqa: E731
    stage("parse events", lambda: load_table(con, "raw_events", j("events", "*.csv"), EVENTS), ["raw_events"])
    stage("parse links", lambda: load_table(con, "links", j("links", "*.csv"), LINKS), ["links"])
    stage("parse clicks", lambda: load_table(con, "raw_clicks", j("clicks", "*.csv"), CLICKS), ["raw_clicks"])
    stage("parse products", lambda: load_table(con, "products", j("products.csv"), ["product_id", "category"]))
    stage("parse campaigns", lambda: load_table(con, "campaigns", j("campaigns.csv"), ["campaign_id", "channel"]))
    stage("linked IDs", lambda: build_person_map(con, "links", scratch), ["person_map"])
    stage("events + sessions", lambda: build_events(con, ws, we, log), ["ev_sessioned", "sessions"])
    stage("user_daily", lambda: build_user_daily(con), ["user_daily"])
    stage("clicks + credit + assists", lambda: build_clicks(con, ws, we, log), ["ck_valid", "session_credit", "session_assist"])
    stage("campaign_daily", lambda: build_campaign_daily(con), ["campaign_daily"])
    stage("self-checks", lambda: self_checks(con, log))

    def write():
        write_parquet(con, "user_daily", USER_DAILY_COLUMNS, os.path.join(out, "user_daily"))
        write_parquet(con, "campaign_daily", CAMPAIGN_DAILY_COLUMNS, os.path.join(out, "campaign_daily"))
        return "ok"
    stage("write parquet", write)
    log(f"[{time.time() - t_all:7.2f}s] total")


if __name__ == "__main__":
    main()
