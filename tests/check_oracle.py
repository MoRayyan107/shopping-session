#!/usr/bin/env python3
"""
Run a pipeline on several datasets, time it, and diff both tables exactly against stored oracle outputs.

  python tests/check_oracle.py --data ../data --oracle ../data/oracle [--pipeline pipeline/pipeline.py]
                               [--build] [--runs 1] dev big3x s2 s4 s15

  --build   write the outputs as the oracle instead of comparing (use with the oracle-v1 worktree's pipeline)
  --runs N  run N times and report the best total (timings are noisy on a laptop)

Each run is time-boxed to 60 s. Logs from the pipeline are parsed for "[  1.23s] stage" lines.
"""
import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from compare import load  # noqa: E402
from run_fixtures import SCHEMAS  # noqa: E402

STAGE_RE = re.compile(r"^\[\s*([0-9.]+)s\] ([^:]+)")
SUBSTEP_RE = re.compile(r"^\s+([0-9.]+)s  (.+)$")


def run(pipeline, data, out, scratch):
    env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(out), SCRATCH_DIR=str(scratch), NCPU="8", MEM_GB="12")
    t = time.time()
    p = subprocess.run([sys.executable, "-I", str(pipeline)], env=env, capture_output=True, text=True,
                       encoding="utf-8", timeout=60)
    wall = time.time() - t
    if p.returncode != 0:
        raise RuntimeError(f"exit {p.returncode}: {p.stderr[-1500:]}")
    if p.stdout.strip():
        raise RuntimeError(f"wrote to stdout: {p.stdout[:200]!r}")
    stages = {m.group(2): float(m.group(1)) for m in map(STAGE_RE.match, p.stderr.splitlines()) if m}
    for m in map(SUBSTEP_RE.match, p.stderr.splitlines()):          # indented sub-steps: "      1.94s  dedup ..."
        if m:
            stages["  " + m.group(2)] = float(m.group(1))
    return wall, stages


def diff(out, oracle):
    lines, ok = [], True
    for table, schema in SCHEMAS.items():
        a, b = load(out, table), load(oracle, table)
        cols = list(schema)
        only_a = a.join(b, on=cols, how="anti", nulls_equal=True)
        only_b = b.join(a, on=cols, how="anti", nulls_equal=True)
        keys = [c for c in ("user_id", "campaign_id", "day") if c in schema]
        dup = a.height - a.n_unique(subset=keys)
        good = not (only_a.height or only_b.height or dup) and a.height == b.height
        ok &= good
        lines.append(f"    {table}: {a.height} vs {b.height} rows, only new {only_a.height}, only oracle {only_b.height}, dup {dup}"
                     + ("" if good else f"\n      new: {only_a.head(3).rows()}\n      oracle: {only_b.head(3).rows()}"))
    return ok, lines


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--oracle", required=True)
    ap.add_argument("--pipeline", default=str(ROOT / "pipeline" / "pipeline.py"))
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--show", help="comma-separated substrings of the stage/sub-step names to report")
    ap.add_argument("datasets", nargs="+")
    a = ap.parse_args()
    all_ok = True
    for name in a.datasets:
        data, oracle = Path(a.data) / name, Path(a.oracle) / name
        with tempfile.TemporaryDirectory() as scratch:
            best = None
            for _ in range(a.runs):
                out = Path(scratch) / "out"
                shutil.rmtree(out, ignore_errors=True)
                wall, stages = run(a.pipeline, data, out, scratch)
                if best is None or stages.get("total", wall) < best[1].get("total", best[0]):
                    best = (wall, stages)
                    if a.build:
                        shutil.rmtree(oracle, ignore_errors=True)
                        shutil.copytree(out, oracle)
                    else:
                        ok, lines = diff(out, oracle)
            wall, stages = best
            if a.show:
                shown = [(k.strip(), v) for k, v in stages.items() if any(s in k for s in a.show.split(","))]
            else:
                shown = [(k, v) for v, k in sorted(((v, k) for k, v in stages.items()
                                                    if k != "total" and not k.startswith("  ")), reverse=True)[:4]]
            summary = ", ".join(f"{k} {v:.2f}s" for k, v in shown)
            if a.build:
                print(f"BUILT {name}: total {stages.get('total', wall):.2f}s  ({summary})")
            else:
                all_ok &= ok
                print(f"{'MATCH' if ok else 'DIFF '} {name}: total {stages.get('total', wall):.2f}s  ({summary})")
                if not ok:
                    print("\n".join(lines))
    if not a.build:
        print("ALL MATCH" if all_ok else "SOME DIFFER")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
