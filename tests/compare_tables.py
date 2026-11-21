#!/usr/bin/env python3
"""
Exact, order-insensitive diff of chosen tables between two output folders (like scripts/compare.py,
which always checks both tables), printing up to N differing rows each way.

  python tests/compare_tables.py OUT ANSWER [--tables user_daily] [--rows 10]
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from compare import load  # noqa: E402
from run_fixtures import SCHEMAS  # noqa: E402


def main():
    args = sys.argv[1:]
    left, right = Path(args[0]), Path(args[1])
    tables, nrows = list(SCHEMAS), 10
    if "--tables" in args:
        tables = args[args.index("--tables") + 1].split(",")
    if "--rows" in args:
        nrows = int(args[args.index("--rows") + 1])
    ok = True
    for table in tables:
        schema = SCHEMAS[table]
        a, b = load(left, table), load(right, table)
        keys = [c for c in ("user_id", "campaign_id", "day") if c in schema]
        only_a = a.join(b, on=list(schema), how="anti", nulls_equal=True).sort(keys)
        only_b = b.join(a, on=list(schema), how="anti", nulls_equal=True).sort(keys)
        dup_a, dup_b = a.height - a.n_unique(subset=keys), b.height - b.n_unique(subset=keys)
        print(f"{table}: rows {a.height} vs {b.height}; only in OUT {only_a.height}; only in ANSWER {only_b.height}; "
              f"duplicate keys {dup_a} vs {dup_b}")
        if only_a.height or only_b.height or dup_a or dup_b:
            ok = False
            print("  only in OUT:");    print(only_a.head(nrows))
            print("  only in ANSWER:"); print(only_b.head(nrows))
    print("MATCH" if ok else "DIFFER")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
