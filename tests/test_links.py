#!/usr/bin/env python3
"""
Stage 3 test: pipeline/links.py.

  1. The Python -> DuckDB return path (components_to_table), on its own: empty, one, small and
     1,000,000-entry lists; every (idx, root) pair must come back exactly, in the right pairing.
  2. Fixtures L1-L4: person names, checked against explicit expectations and the expected user_daily names.
  3. Random graphs (400-ID chains in random order, 5,000-ID hubs, repeats, reversed lines, self-links,
     NULL sides, upper-case / tab / space / non-ASCII IDs) against a separate BFS reference.
  4. person_map used the way the pipeline will use it (LEFT JOIN + COALESCE).
  5. Timing at a size larger than the generator's.

Run: python tests/test_links.py
"""
import csv
import random
import sys
import tempfile
import time
from collections import defaultdict, deque
from pathlib import Path

import duckdb
import polars as pl

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
from csv_parse import load_table  # noqa: E402
from links import build_person_map, components_to_table, union_find  # noqa: E402

failures = []


def check(name, ok, detail=""):
    print(("ok   " if ok else "FAIL ") + name)
    if not ok:
        failures.append(name)
        if detail != "":
            print("    " + str(detail)[:600])


def bykey(s):
    return s.encode("utf-8")


def ref_map(pairs):
    """Independent reference: BFS components over the usable edges -> {id: person} for ids with id != person."""
    adj = defaultdict(set)
    for a, b in pairs:
        if a is None or b is None or a == b:
            continue
        adj[a].add(b)
        adj[b].add(a)
    seen, out = set(), {}
    for start in adj:
        if start in seen:
            continue
        comp, q = [], deque([start])
        seen.add(start)
        while q:
            x = q.popleft()
            comp.append(x)
            for y in adj[x]:
                if y not in seen:
                    seen.add(y)
                    q.append(y)
        name = min(comp, key=bykey)
        for x in comp:
            if x != name:
                out[x] = name
    return out


def load_links(con, pairs):
    """Load test pairs through Parquet (list query parameters are far too slow, see links.components_to_table)."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "links.parquet"
        pl.DataFrame({"user_id_a": [a for a, _ in pairs], "user_id_b": [b for _, b in pairs]},
                     schema={"user_id_a": pl.String, "user_id_b": pl.String}).write_parquet(path)
        con.execute(f"CREATE OR REPLACE TABLE links AS SELECT user_id_a, user_id_b FROM read_parquet('{path.as_posix()}')")


def get_map(con):
    return dict(con.execute("SELECT id, person FROM person_map").fetchall())


def main():
    con = duckdb.connect()
    print("duckdb", duckdb.__version__)

    # ---------- 1. the Python -> DuckDB return path
    rng = random.Random(3)
    for n in (0, 1, 2, 10, 1000):
        roots = [rng.randrange(n) for _ in range(n)] if n else []
        components_to_table(con, "rt", roots)
        rows = con.execute("SELECT idx, root FROM rt ORDER BY idx").fetchall()
        check(f"return path n={n}: every (idx, root) pair comes back, correctly paired", rows == list(enumerate(roots)), rows[:5])
    n = 1_000_000
    roots = list(range(n))
    rng.shuffle(roots)                                       # a permutation: pairing errors cannot hide
    t = time.time()
    components_to_table(con, "rt", roots)
    took = time.time() - t
    cnt, mn, mx, s_idx, s_root, s_prod = con.execute(
        "SELECT count(*), min(root), max(root), sum(idx::HUGEINT), sum(root::HUGEINT), sum(idx::HUGEINT * root) FROM rt").fetchone()
    check(f"return path n=1,000,000 ({took:.2f}s): count, range and sums", (cnt, mn, mx, s_idx, s_root) == (n, 0, n - 1, n * (n - 1) // 2, n * (n - 1) // 2), (cnt, mn, mx))
    check("return path n=1,000,000: sum(idx*root) matches (pairing preserved)", s_prod == sum(i * r for i, r in enumerate(roots)))
    back = [r for (r,) in con.execute("SELECT root FROM rt ORDER BY idx").fetchall()]
    check("return path n=1,000,000: exact list equality after ORDER BY idx", back == roots)
    check("return path: column types are INTEGER", [c[1] for c in con.execute("DESCRIBE rt").fetchall()] == ["INTEGER", "INTEGER"])
    check("return path: duplicate idx / NULLs absent", con.execute("SELECT count(DISTINCT idx), count(*) FILTER (WHERE root IS NULL) FROM rt").fetchone() == (n, 0))

    # ---------- union-find alone
    check("union_find: chain of 5", len(set(union_find(5, [(0, 1), (1, 2), (2, 3), (3, 4)]))) == 1)
    check("union_find: no edges", union_find(3, []) == [0, 1, 2])
    deep = union_find(200_000, [(i, i + 1) for i in range(199_999)])
    check("union_find: 200,000-link chain, no recursion problem", len(set(deep)) == 1)

    # ---------- 2. fixtures L1-L4
    fx = ROOT / "fixtures"
    expect = {
        "L1": {"u2": "u1"},
        "L2": {"u3": "U7", "u9": "U7", "u2": "U7"},                   # U7 is the smallest and never has an event
        "L3": {"u4": "A\tB", "u1": "A\tB", "user_id_a": "A\tB", "user_id_b": "A\tB"},
        "L4": {"u2": "u1"},
    }
    for case, want in expect.items():
        load_table(con, "links", str(fx / case / "input" / "links" / "*.csv"), ["user_id_a", "user_id_b"])
        build_person_map(con)
        got = get_map(con)
        check(f"{case}: person_map", got == want, f"got {got}")
        # the expected user_daily names must all be persons, never a mapped (non-smallest) ID
        names = {r["user_id"] for r in csv.DictReader(open(fx / case / "expected" / "user_daily.csv", encoding="utf-8", newline=""))}
        check(f"{case}: no expected row is named by an ID that person_map renames", not (names & set(got)), names & set(got))

    # ---------- 3. random graphs vs the BFS reference
    weird = ["U1", "u1 ", " u1", "A\tB", "é", "€x", "\U0001F600", "u", "ZZ", "a" * 30, "a" * 30 + "B"]
    for seed in range(1, 31):
        rng = random.Random(seed)
        pairs = []
        pool = [f"u{rng.randrange(10**6)}" for _ in range(3000)] + weird
        # chains of 50..400 IDs in a random order
        for _ in range(rng.randrange(1, 5)):
            chain = rng.sample(pool, rng.randrange(50, 401)) if len(pool) >= 401 else pool[:]
            pairs += list(zip(chain, chain[1:]))
        # a hub: one device ID linked to thousands
        hub = rng.choice(pool)
        pairs += [(hub, x) for x in rng.sample(pool, 2500)]
        # random extra edges, repeats both ways round, self-links, NULL sides
        for _ in range(500):
            a, b = rng.choice(pool), rng.choice(pool)
            pairs.append((a, b))
            if rng.random() < 0.3:
                pairs.append((b, a))
            if rng.random() < 0.2:
                pairs.append((a, a))
            if rng.random() < 0.1:
                pairs.append((a, None))
            if rng.random() < 0.1:
                pairs.append((None, b))
        rng.shuffle(pairs)
        load_links(con, pairs)
        build_person_map(con)
        got, want = get_map(con), ref_map(pairs)
        check(f"random graph seed {seed}: person_map equals the BFS reference ({len(pairs)} lines, {len(want)} renamed IDs)", got == want,
              f"missing {list(set(want.items()) - set(got.items()))[:3]} extra {list(set(got.items()) - set(want.items()))[:3]}")

    # a long chain in a random order, alone: one person named by the smallest ID
    ids = [f"id{i:04d}" for i in range(400)]
    shuffled = ids[:]
    random.Random(1).shuffle(shuffled)
    load_links(con, list(zip(shuffled, shuffled[1:])))
    build_person_map(con)
    got = get_map(con)
    check("400-ID chain in random order: all 399 other IDs map to id0000", len(got) == 399 and set(got.values()) == {"id0000"})
    # hub
    hubids = [f"d{i:05d}" for i in range(5000)]
    load_links(con, [("device", x) for x in hubids])
    build_person_map(con)
    got = get_map(con)
    check("hub of 5,000 IDs: everything maps to the smallest ('d00000'), 'device' included", len(got) == 5000 and set(got.values()) == {"d00000"} and got["device"] == "d00000")

    # ---------- 4. used the way the pipeline will
    load_links(con, [("u2", "u1"), ("u3", "u9"), ("u9", "u2"), ("U7", "u9"), ("u10", "u10"), ("u6", "u6"), ("u5", None), (None, "u4")])
    build_person_map(con)
    con.execute("CREATE OR REPLACE TABLE ev AS SELECT unnest(['u1','u2','u3','u10','u5','u4','u6','u99', 'U1']) AS user_id")
    got = dict(con.execute("SELECT e.user_id, COALESCE(m.person, e.user_id) FROM ev e LEFT JOIN person_map m ON m.id = e.user_id").fetchall())
    # u2-u1, u3-u9, u9-u2, U7-u9 make u1, u2, u3, u9, U7 one person, named U7
    want = {"u1": "U7", "u2": "U7", "u3": "U7", "u10": "u10", "u5": "u5", "u4": "u4", "u6": "u6", "u99": "u99", "U1": "U1"}
    check("LEFT JOIN + COALESCE: linked, self-linked, NULL-side, unlinked and upper-case IDs", got == want, got)

    # empty / no usable edges
    load_links(con, [("a", "a"), ("b", None), (None, None)])
    check("only unusable lines: person_map is empty and the build does not fail", build_person_map(con) == 0)
    con.execute("CREATE OR REPLACE TABLE links AS SELECT NULL::VARCHAR AS user_id_a, NULL::VARCHAR AS user_id_b WHERE false")
    check("empty links table: person_map is empty", build_person_map(con) == 0)

    # ---------- 5. timing, bigger than the generator's shape
    rng = random.Random(99)
    pairs = []
    k = 0
    for _ in range(40000):                                       # 40k small people (trees)
        m = rng.randrange(2, 8)
        ids = [f"p{k + j}" for j in range(m)]
        k += m
        pairs += [(ids[j], ids[rng.randrange(j)]) for j in range(1, m)]
    for c in range(60):                                          # 60 chains of 400
        ids = [f"c{c}_{j}" for j in range(400)]
        rng.shuffle(ids)
        pairs += list(zip(ids, ids[1:]))
    for h in range(6):                                           # 6 devices linking 3,000 IDs each
        pairs += [(f"dev{h}", f"p{rng.randrange(k)}") for _ in range(3000)]
    rng.shuffle(pairs)
    load_links(con, pairs)
    t = time.time()
    build_person_map(con)
    took = time.time() - t
    check(f"scale: {len(pairs)} link lines -> person_map in {took:.2f}s, equals the BFS reference", get_map(con) == ref_map(pairs))

    print("\nALL OK" if not failures else f"\n{len(failures)} FAILED: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
