#!/usr/bin/env python3
"""
Add ev_valid (events after dedup, validation and the late filter) to cached oracle dumps, using the oracle's
own build_events (from an oracle worktree) on the cached raw_events dump.

  python tests/oracle_ev_valid.py --oracle-src ORACLE/pipeline --data ../data --cache ../data NAME ...
"""
import argparse
import json
import os
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--oracle-src", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--cache", required=True)
    ap.add_argument("names", nargs="+")
    a = ap.parse_args()
    sys.path.insert(0, a.oracle_src)
    from csv_parse import load_table  # noqa: E402  (the oracle's modules)
    from events import build_events  # noqa: E402
    if a.names == ["fx/ALL"]:
        a.names = [f"fx/{c}" for c in sorted(os.listdir(ROOT / "fixtures")) if (ROOT / "fixtures" / c).is_dir()]
    for name in a.names:
        data = ROOT / "fixtures" / name[3:] / "input" if name.startswith("fx/") else Path(a.data) / name
        dumps = Path(a.cache) / "oracle_dumps" / name
        con = duckdb.connect()
        con.execute("SET threads = 8")
        con.execute(Path(a.oracle_src, "ts_macros.sql").read_text(encoding="utf-8"))
        manifest = json.loads((data / "manifest.json").read_text(encoding="utf-8"))
        ws, we = (con.execute("SELECT ts_parse(?)", [manifest[k]]).fetchone()[0] for k in ("window_start", "window_end"))
        con.execute(f"CREATE TABLE raw_events AS SELECT * FROM read_parquet('{(dumps / 'raw_events.parquet').as_posix()}')")
        con.execute("CREATE TABLE person_map (id VARCHAR, person VARCHAR)")
        load_table(con, "products", str(data / "products.csv"), ["product_id", "category"])
        build_events(con, ws, we)
        con.execute(f"COPY ev_valid TO '{(dumps / 'ev_valid.parquet').as_posix()}' (FORMAT parquet)")
        n = con.execute("SELECT count(*) FROM ev_valid").fetchone()[0]
        print(f"ok   {name}: ev_valid {n} rows", flush=True)


if __name__ == "__main__":
    main()
