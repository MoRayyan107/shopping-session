#!/usr/bin/env python3
"""
Run the pipeline (locally, no Docker) on every fixture and print only the rows that differ.

  python tests/fixtures_diff.py [--tables user_daily,campaign_daily] [CASE ...]

Uses the starter's own loaders (scripts/run_fixtures.py), so the schema checks are the scorer-style ones.
"""
import os
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from run_fixtures import SCHEMAS, load_actual, load_expected  # noqa: E402


def fmt(row):
    return "(" + ", ".join(repr(v) if isinstance(v, str) else str(v) for v in row) + ")"


def main():
    args = sys.argv[1:]
    tables = list(SCHEMAS)
    if args and args[0] == "--tables":
        tables, args = args[1].split(","), args[2:]
    fx = ROOT / "fixtures"
    cases = args or sorted(p.name for p in fx.iterdir() if p.is_dir())
    passed = 0
    for case in cases:
        data = fx / case / "input"
        with tempfile.TemporaryDirectory() as out, tempfile.TemporaryDirectory() as scratch:
            env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=out, SCRATCH_DIR=scratch, NCPU="2", MEM_GB="2")
            p = subprocess.run([sys.executable, "-I", str(ROOT / "pipeline" / "pipeline.py")], env=env,
                               capture_output=True, text=True, encoding="utf-8")
            if p.returncode != 0:
                print(f"FAIL {case}: exit {p.returncode}\n{p.stderr[-1500:]}")
                continue
            if p.stdout.strip():
                print(f"FAIL {case}: wrote to stdout: {p.stdout[:200]!r}")
            lines = []
            for table in tables:
                schema = SCHEMAS[table]
                exp = Counter(load_expected(fx / case / "expected" / f"{table}.csv", schema).rows())
                try:
                    act = Counter(load_actual(Path(out) / table, schema).rows())
                except Exception as e:  # noqa: BLE001
                    lines.append(f"  {table}: {e}")
                    continue
                for row in sorted(exp - act, key=repr):
                    lines.append(f"  {table} only expected: {fmt(row)}")
                for row in sorted(act - exp, key=repr):
                    lines.append(f"  {table} only actual:   {fmt(row)}")
            if lines:
                print(f"DIFF {case}")
                print("\n".join(lines))
            else:
                passed += 1
                print(f"ok   {case}")
    print(f"\n{passed}/{len(cases)} fixtures match on {','.join(tables)}")
    return 0 if passed == len(cases) else 1


if __name__ == "__main__":
    sys.exit(main())
