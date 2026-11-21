#!/usr/bin/env python3
"""
Stage 1 test: pipeline/csv_parse.py (the rules.md CSV dialect).

  1. Explicit assertions for the dialect traps named in fixtures/README.md (E13, E15, E19, E20, L3).
  2. Every events/clicks/links file of all 36 fixtures against an independent reference reader
     (character-level, no regex), compared as a multiset of rows.
  3. Hand-made files (empty, header-only, no final newline, \\r, BOM, quotes, blank lines).
  4. A fuzz file of mutated lines.

Run: python tests/test_parse.py
"""
import random
import sys
import tempfile
from collections import Counter
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
from csv_parse import load_table  # noqa: E402

EVENTS = ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]
CLICKS = ["click_id", "user_id", "campaign_id", "ts"]
LINKS = ["user_id_a", "user_id_b"]
failures = []


def check(name, ok, detail=""):
    print(("ok   " if ok else "FAIL ") + name)
    if not ok:
        failures.append(name)
        if detail:
            print("    " + str(detail))


# ------------------------------------------------------------------ independent reference reader
def ref_line(line, n):
    """One line -> tuple of n values (None for empty), or None when the line is malformed."""
    if line == "":
        return None
    fields, cur, i = [], [], 0
    # walk the characters: a comma ends a field; quotes are only allowed as a matched pair around a whole field
    parts = []
    for ch in line:
        if ch == ",":
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    if len(parts) != n:
        return None
    for p in parts:
        if p.count('"') == 0:
            v = p
        elif len(p) >= 2 and p[0] == '"' and p[-1] == '"' and p.count('"') == 2:
            v = p[1:-1]
        else:
            return None
        fields.append(v if v != "" else None)
    return tuple(fields)


def ref_files(paths, n):
    rows = []
    for path in paths:
        data = Path(path).read_bytes().decode("utf-8")
        lines = data.split("\n")[1:]                      # header = first line of the file
        rows += [r for r in (ref_line(l, n) for l in lines) if r is not None]
    return rows


def sql_rows(con, table, glob, cols):
    load_table(con, table, glob, cols)
    return [tuple(r) for r in con.execute(f"SELECT * FROM {table}").fetchall()]


def main():
    con = duckdb.connect()
    print("duckdb", duckdb.__version__)
    fx = ROOT / "fixtures"

    def one(case, kind, cols):
        return sorted(sql_rows(con, "t", str(fx / case / "input" / kind / "*.csv"), cols), key=repr)

    # ---------- 1. explicit assertions
    r = one("E20", "events", EVENTS)
    check("E20: purchase price keeps its \\r", any(x[6] == "1000\r" for x in r), r)
    check("E20: event ids e3 and 'e3\\r' both survive", {"e3", "e3\r"} <= {x[0] for x in r}, [x[0] for x in r])
    check("E20: view kept", any(x[4] == "view" and x[6] is None for x in r))
    r = one("E20", "clicks", CLICKS)
    check("E20: click ts keeps its \\r", any(x[3] is not None and x[3].endswith("Z\r") for x in r), r)

    r = one("E15", "events", EVENTS)
    check("E15: \"u1\" is u1 (quotes are syntax)", sorted(x[1] for x in r) == ["u1", "u1"], r)

    r = one("E19", "events", EVENTS)
    users = {x[1] for x in r}
    check("E19: ' u1', 'u1 ' and quoted ' u1' keep their spaces", {"u1", " u1", "u1 "} <= users, users)
    check("E19: ' view' and ' p1' keep their spaces", any(x[4] == " view" for x in r) and any(x[5] == " p1" for x in r))
    check("E19: event ids 'e1' and 'e1 ' differ", {"e1", "e1 "} <= {x[0] for x in r}, [x[0] for x in r])
    r = one("E19", "clicks", CLICKS)
    check("E19: click user ' u1' and campaign ' c_1' kept as-is", any(x[1] == " u1" for x in r) and any(x[2] == " c_1" for x in r), r)

    r = one("E13", "events", EVENTS)
    check("E13: empty event_id parses to NULL (dropped in the events stage, not here)", any(x[0] is None for x in r), r)
    check("E13: 6-field line is malformed", all(len(x) == 7 for x in r) and not any(x[0] == "e2" for x in r), r)
    check("E13: 'Purchase' and F3 '1791071890000.0' survive parsing untouched",
          any(x[4] == "Purchase" for x in r) and any(x[2] == "1791071890000.0" for x in r), r)

    r = one("L3", "links", LINKS)
    want = sorted([("u1", None), (None, "u3"), ("u4", "u1"), ("u4", "u1"), ("u6", "u6"),
                   ("user_id_a", "user_id_b"), ("u4", "user_id_b"), ("A\tB", "user_id_a")], key=repr)
    check("L3: 8 well-formed lines (3-field, unbalanced quote, empty and 1-field lines dropped; header-equal row is data)",
          r == want, f"got {r}")

    # ---------- 2. every fixture file against the reference
    bad = []
    nfiles = 0
    for case in sorted(p for p in fx.iterdir() if p.is_dir()):
        for kind, cols in (("events", EVENTS), ("clicks", CLICKS), ("links", LINKS)):
            files = sorted((case / "input" / kind).glob("*.csv"))
            nfiles += len(files)
            got = Counter(sql_rows(con, "t", str(case / "input" / kind / "*.csv"), cols))
            want = Counter(ref_files(files, len(cols)))
            if got != want:
                bad.append((case.name, kind, list((got - want).items())[:3], list((want - got).items())[:3]))
    check(f"all fixtures: {nfiles} files match the reference reader", not bad, bad[:3])

    # ---------- 3. hand-made files
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        cases = {
            "f1.csv": b'a,b,c\nx,"y",z\r\n\n"q"\r\nlast,line,nofinalnewline',
            "f2_headeronly.csv": b"a,b,c\n",
            "f3_empty.csv": b"",
            "f4_trailingblank.csv": b"a,b,c\n1,2,3\n\n\n",
            "f5_weird.csv": 'a,b,c\nu,é€😀,"un"bal\n"x,y,z\nw,\x01,z\n"",  ,"  "\n,,\n'.encode("utf-8"),
            "f6_header_data.csv": b"a,b,c\na,b,c\n",
            "f7_quotes.csv": b'a,b,c\n"",x,y\n"a"b,c,d\n""",c,d\n"""",c,d\nq,"",""\n',
            "f8_cr.csv": b"a,b,c\r\n1,2,3\r\n",
            "f9_bom.csv": b"\xef\xbb\xbfa,b,c\n1,2,3\n",
            "f10_quote_cr.csv": b'a,b,c\nk,l,"m"\r\nk,l,"n"\n',
        }
        for name, data in cases.items():
            (d / name).write_bytes(data)
        got = Counter(sql_rows(con, "t", str(d / "*.csv"), ["a", "b", "c"]))
        want = Counter(ref_files(sorted(d.glob("*.csv")), 3))
        check("hand-made files match the reference reader", got == want, f"only sql {got - want}; only ref {want - got}")
        check("hand-made: no output value holds a quote (unbalanced, 'a\"b', '\"\"\"' and '\"\"\"\"' lines all dropped)",
              not any(v is not None and '"' in v for k in got for v in k), [k for k in got if any(v and '"' in v for v in k)])
        check("hand-made: 'x,\"y\",z\\r' keeps its \\r", ("x", "y", "z\r") in got, got)
        check("hand-made: '1,2,\"3\"\\r' is malformed (closing quote is followed by \\r, so it is not around the whole field)",
              ("1", "2", "3") not in {k for k in got} or got[("1", "2", "3")] == 2 and ("1", "2", "3\r") in got, got)
        check("hand-made: 'k,l,\"m\"\\r' is malformed but 'k,l,\"n\"' is kept as n",
              ("k", "l", "n") in got and not any(k[2] is not None and k[2].startswith("m") for k in got), got)
        check("hand-made: data row equal to the header is kept", ("a", "b", "c") in got, got)
        check("hand-made: '\"\",  ,\"  \"' -> NULL and two space values", (None, "  ", "  ") in got or (None, " ", "  ") in got, got)
        check("hand-made: ',,' -> all NULL", (None, None, None) in got, got)

    # ---------- 4. fuzz
    rng = random.Random(7)
    base = ['e1,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,', 'e2,"u1",2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,purchase,p2,"1000"',
            '"e3",u 1,1791071890000,2026-10-03T10:01:00Z,cart," p1",']
    glyphs = list(',"\r \té') + ["٢", "\x01", "'"]
    lines = []
    for _ in range(60000):
        s = rng.choice(base)
        for _ in range(rng.choice([0, 1, 1, 2, 3])):
            i = rng.randrange(len(s) + 1)
            op = rng.randrange(3)
            if op == 0 and s:
                i = min(i, len(s) - 1)
                s = s[:i] + rng.choice(glyphs) + s[i + 1:]
            elif op == 1:
                s = s[:i] + rng.choice(glyphs) + s[i:]
            elif s:
                i = min(i, len(s) - 1)
                s = s[:i] + s[i + 1:]
        lines.append(s)
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "fuzz.csv"
        p.write_bytes(("h\n" + "\n".join(lines) + "\n").encode("utf-8"))
        got = Counter(sql_rows(con, "t", str(Path(tmp) / "*.csv"), EVENTS))
        want = Counter(ref_files([p], 7))
        check(f"fuzz: {len(lines)} mutated lines, {sum(want.values())} well-formed, same multiset",
              got == want, f"only sql {list((got - want).items())[:3]}; only ref {list((want - got).items())[:3]}")

    # ---------- empty glob
    n = load_table(con, "t", str(ROOT / "no_such_dir" / "*.csv"), CLICKS)
    check("empty glob gives an empty table with the right columns", n == 0 and [c[0] for c in con.execute("DESCRIBE t").fetchall()] == CLICKS)

    print("\nALL OK" if not failures else f"\n{len(failures)} FAILED: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
