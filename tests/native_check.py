#!/usr/bin/env python3
"""
Run the native binary (linux/amd64, in Docker) on datasets with NATIVE_DUMP_DIR, then diff the chosen stage
tables exactly (as multisets, EXCEPT ALL both ways) against the cached oracle dumps.

  python tests/native_check.py --tables raw_events,raw_clicks,links [--outputs] NAME ...

NAME: a folder under ../data (dev, small/s62, p20m) or fx/<CASE>; fx/ALL = all 36 fixtures; ALL = everything
except p20m. --outputs also diffs the final Parquet tables against the cached oracle outputs.
Binary: /target/release/shopping_sessions in the native-target Docker volume (build it first).
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA = (ROOT.parent / "data").resolve()
DUMPS = DATA / "oracle_dumps"
ORACLE_OUT = DATA / "oracle"
IMAGE = "rust:1.85.0-slim-bookworm"


def expand(names):
    out = []
    fx = sorted(c for c in os.listdir(ROOT / "fixtures") if (ROOT / "fixtures" / c).is_dir())
    for n in names:
        if n == "fx/ALL":
            out += [f"fx/{c}" for c in fx]
        elif n == "ALL":
            out += [f"fx/{c}" for c in fx] + ["dev", "big3x", "s2", "s4", "s15"] + \
                   sorted(f"small/{d}" for d in os.listdir(DATA / "small"))
        else:
            out.append(n)
    return out


def run_native(names, work, ncpu, tables):
    script = [f"export NATIVE_PROFILE=1 NATIVE_DUMP_TABLES={','.join(tables) or 'none'}"]
    for n in names:
        src = f"/fx/{n[3:]}/input" if n.startswith("fx/") else f"/data/{n}"
        safe = n.replace("/", "__")
        script.append(f'mkdir -p /work/{safe}/out /work/{safe}/dumps && DATA_DIR={src} OUTPUT_DIR=/work/{safe}/out '
                      f'NATIVE_DUMP_DIR=/work/{safe}/dumps NCPU={ncpu} /target/release/shopping_sessions '
                      f'2> /work/{safe}/log.txt || echo "FAILED {n}: exit $?"')
    cmd = ["docker", "run", "--rm", "--platform", "linux/amd64", "--network", "none",
           "-v", "native-target:/target:ro", "-v", f"{DATA}:/data:ro", "-v", f"{ROOT / 'fixtures'}:/fx:ro",
           "-v", "vol_p20m:/data/p20m:ro", "-v", f"{work}:/work", IMAGE, "sh", "-c", "\n".join(script)]
    p = subprocess.run(cmd, capture_output=True, text=True, env=dict(os.environ, MSYS_NO_PATHCONV="1"))
    if p.returncode != 0 or p.stdout.strip():
        print(p.stdout, p.stderr)


def diff_table(con, native, oracle, show=3):
    """native/oracle: a Parquet file, or a folder (all its *.parquet files)."""
    if not native.exists() or (native.is_dir() and not any(native.glob("*.parquet"))):
        return False, f"native table missing: {native.name}"
    native = native / "*.parquet" if native.is_dir() else native
    oracle = oracle / "*.parquet" if oracle.is_dir() else oracle
    ocols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{oracle.as_posix()}')").fetchall()
    ncols = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{native.as_posix()}')").fetchall()}
    if ncols != {c[0] for c in ocols}:
        return False, f"columns differ: native {sorted(ncols)} vs oracle {sorted(c[0] for c in ocols)}"
    sel_n = ", ".join(f'CAST("{c}" AS {t}) AS "{c}"' for c, t, *_ in ocols)
    sel_o = ", ".join(f'"{c}"' for c, *_ in ocols)
    qn = f"SELECT {sel_n} FROM read_parquet('{native.as_posix()}')"
    qo = f"SELECT {sel_o} FROM read_parquet('{oracle.as_posix()}')"
    nn, no = (con.execute(f"SELECT count(*) FROM ({q})").fetchone()[0] for q in (qn, qo))
    only_n = con.execute(f"SELECT count(*) FROM ({qn} EXCEPT ALL {qo})").fetchone()[0]
    only_o = con.execute(f"SELECT count(*) FROM ({qo} EXCEPT ALL {qn})").fetchone()[0]
    msg = f"{nn} vs {no} rows, only native {only_n}, only oracle {only_o}"
    if only_n or only_o:
        msg += f"\n        native: {con.execute(f'{qn} EXCEPT ALL {qo} LIMIT {show}').fetchall()}"
        msg += f"\n        oracle: {con.execute(f'{qo} EXCEPT ALL {qn} LIMIT {show}').fetchall()}"
    return not (only_n or only_o), msg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", default="")
    ap.add_argument("--outputs", action="store_true")
    ap.add_argument("--ncpu", default="8")
    ap.add_argument("--keep", help="keep the native outputs and dumps in this folder")
    ap.add_argument("names", nargs="+")
    a = ap.parse_args()
    names = expand(a.names)
    tables = [t for t in a.tables.split(",") if t]
    work = Path(a.keep) if a.keep else Path(tempfile.mkdtemp(prefix="native_check_"))
    work.mkdir(parents=True, exist_ok=True)
    run_native(names, work, a.ncpu, tables)
    con = duckdb.connect()
    bad = 0
    for n in names:
        safe = n.replace("/", "__")
        lines, ok = [], True
        if not (work / safe / "log.txt").exists() or "total" not in (work / safe / "log.txt").read_text(encoding="utf-8", errors="replace"):
            ok = False
            lines.append("    native run failed:\n" + (work / safe / "log.txt").read_text(encoding="utf-8", errors="replace")[-1500:]
                         if (work / safe / "log.txt").exists() else "    no log")
        else:
            for t in tables:
                good, msg = diff_table(con, work / safe / "dumps" / f"{t}.parquet", DUMPS / n / f"{t}.parquet")
                ok &= good
                lines.append(f"    {t}: {msg}")
            if a.outputs:
                for t in ("user_daily", "campaign_daily"):
                    good, msg = diff_table(con, work / safe / "out" / t, ORACLE_OUT / n / t)
                    ok &= good
                    lines.append(f"    output {t}: {msg}")
        bad += not ok
        print(f"{'MATCH' if ok else 'DIFF '} {n}")
        if not ok or len(names) <= 6:
            print("\n".join(lines))
    print(f"\n{len(names) - bad}/{len(names)} datasets match on {','.join(tables + (['outputs'] if a.outputs else []))}")
    if not a.keep:
        shutil.rmtree(work, ignore_errors=True)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
