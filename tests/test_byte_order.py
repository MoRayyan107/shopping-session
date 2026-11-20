#!/usr/bin/env python3
"""
Does DuckDB order VARCHAR byte-wise (UTF-8 bytes), as rules.md requires?

Checks ORDER BY, min/max, comparison operators, min_by/arg_min, GROUP BY + min, a window
ordering, and a multi-threaded sort, against a Python reference that sorts on .encode("utf-8").
Run: python tests/test_byte_order.py
"""
import sys

import duckdb

IDS = [
    "u1", "U1", "u10", "u2", "U7", "u",
    " u1", "u1 ", "\tu1", "A\tB", "A B", "A", "a",
    "user_id_a", "user_id_b", "_", "~", "0", "9", "-", "Z", "z",
    "é", "É", "é",            # U+00E9 (C3 A9), U+00C9, e + combining accent
    "€",                             # E2 82 AC
    "Ａ",                        # full-width A, EF BC A1
    "",                        # EE 80 80  (in UTF-16 this sorts AFTER U+10000)
    "\U00010000",                    # F0 90 80 80 (UTF-16 surrogates D800 DC00)
    "😀",                            # F0 9F 98 80
    "\U0010ffff",                    # F4 8F BF BF, the largest code point
    "x" * 11 + "a", "x" * 11 + "b", "x" * 12 + "a", "x" * 12 + "B",   # around DuckDB's 12-byte inline limit
    "x" * 40 + "a", "x" * 40 + "B", "x" * 40,
]


def ref_sorted(values):
    return sorted(values, key=lambda s: s.encode("utf-8"))


def main():
    failures = []

    def check(name, got, want):
        ok = got == want
        print(("ok   " if ok else "FAIL ") + name)
        if not ok:
            failures.append(name)
            print("   got :", [repr(x) for x in got] if isinstance(got, list) else repr(got))
            print("   want:", [repr(x) for x in want] if isinstance(want, list) else repr(want))

    con = duckdb.connect()
    print("duckdb", duckdb.__version__)
    con.execute("SET threads = 8")
    con.execute("CREATE TABLE t(id VARCHAR, k INTEGER)")
    con.executemany("INSERT INTO t VALUES (?, ?)", [(s, i) for i, s in enumerate(IDS)])
    want = ref_sorted(IDS)

    check("ORDER BY id", [r[0] for r in con.execute("SELECT id FROM t ORDER BY id").fetchall()], want)
    check("ORDER BY id DESC", [r[0] for r in con.execute("SELECT id FROM t ORDER BY id DESC").fetchall()], want[::-1])
    check("min(id)", con.execute("SELECT min(id) FROM t").fetchone()[0], want[0])
    check("max(id)", con.execute("SELECT max(id) FROM t").fetchone()[0], want[-1])
    check("min_by(id, id)", con.execute("SELECT min_by(id, id) FROM t").fetchone()[0], want[0])
    check("arg_min(id, id)", con.execute("SELECT arg_min(id, id) FROM t").fetchone()[0], want[0])
    check("list_sort", con.execute("SELECT list_sort(list(id)) FROM t").fetchone()[0], want)
    check("window row_number order", [r[0] for r in con.execute(
        "SELECT id FROM (SELECT id, row_number() OVER (ORDER BY id) rn FROM t) ORDER BY rn").fetchall()], want)

    # pairwise comparison operators agree with the reference for every pair
    bad = []
    rows = con.execute("SELECT a.id, b.id, a.id < b.id, a.id = b.id FROM t a, t b").fetchall()
    for a, b, lt, eq in rows:
        if lt != (a.encode() < b.encode()) or eq != (a == b):
            bad.append((a, b))
    check(f"pairwise < and = over {len(rows)} pairs", bad, [])

    # the way the pipeline will use it: smallest ID per group
    con.execute("CREATE TABLE g AS SELECT id, k % 3 AS grp FROM t")
    got = dict(con.execute("SELECT grp, min(id) FROM g GROUP BY grp").fetchall())
    ref = {}
    for i, s in enumerate(IDS):
        ref[i % 3] = min(ref.get(i % 3, s), s, key=lambda x: x.encode("utf-8"))
    check("GROUP BY min(id)", got, ref)

    # a large multi-threaded sort, so the parallel merge path is exercised
    con.execute("CREATE TABLE big AS SELECT id || CAST(i AS VARCHAR) AS id FROM t, range(2000) r(i)")
    got = [r[0] for r in con.execute("SELECT id FROM big ORDER BY id").fetchall()]
    want_big = ref_sorted([s + str(i) for s in IDS for i in range(2000)])
    check(f"large ORDER BY ({len(got)} rows)", got == want_big, True)

    # the three things rules.md names: uppercase before lowercase, tab before letters, non-ASCII after ASCII
    check("'U1' < 'u1'", con.execute("SELECT 'U1' < 'u1'").fetchone()[0], True)
    check("'A\\tB' < 'u1'", con.execute("SELECT 'A\t' || 'B' < 'u1'").fetchone()[0], True)
    check("'é' > 'z'", con.execute("SELECT 'é' > 'z'").fetchone()[0], True)

    print("\nALL OK" if not failures else f"\n{len(failures)} FAILED: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
