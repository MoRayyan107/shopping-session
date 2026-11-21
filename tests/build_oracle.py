#!/usr/bin/env python3
"""
Run the DuckDB oracle on datasets and cache its outputs and stage dumps (PIPELINE_DUMP_DIR).

  python tests/build_oracle.py --pipeline ORACLE_PIPELINE --data ../data --cache ../data NAME ...

NAME is a dataset folder under --data (e.g. dev, small/s62), or fx/<CASE> for a fixture (fixtures/<CASE>/input).
Outputs go to <cache>/oracle/<NAME>/ and stage dumps to <cache>/oracle_dumps/<NAME>/.
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def data_dir(name, data):
    if name.startswith("fx/"):
        return ROOT / "fixtures" / name[3:] / "input"
    return Path(data) / name


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pipeline", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--cache", required=True)
    ap.add_argument("names", nargs="+")
    a = ap.parse_args()
    if a.names == ["fx/ALL"]:
        a.names = [f"fx/{c}" for c in sorted(os.listdir(ROOT / "fixtures")) if (ROOT / "fixtures" / c).is_dir()]
    failed = 0
    for name in a.names:
        out = Path(a.cache) / "oracle" / name
        dumps = Path(a.cache) / "oracle_dumps" / name
        shutil.rmtree(out, ignore_errors=True)
        shutil.rmtree(dumps, ignore_errors=True)
        with tempfile.TemporaryDirectory() as scratch:
            env = dict(os.environ, DATA_DIR=str(data_dir(name, a.data)), OUTPUT_DIR=str(out), SCRATCH_DIR=scratch,
                       PIPELINE_DUMP_DIR=str(dumps), NCPU="8", MEM_GB="16")
            t = time.time()
            p = subprocess.run([sys.executable, "-I", a.pipeline], env=env, capture_output=True, text=True, encoding="utf-8")
            ok = p.returncode == 0 and "self-checks: ok" in p.stderr
            failed += not ok
            print(f"{'ok  ' if ok else 'FAIL'} {name}: {time.time() - t:.1f}s" + ("" if ok else "\n" + p.stderr[-1500:]), flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
