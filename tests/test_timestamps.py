#!/usr/bin/env python3
"""
Standalone test of pipeline/ts_macros.sql (ts_parse) on a table of timestamps.

1. A hand-written table: every form and every rules.md trap, with the expected epoch ms (or NULL).
   Expected values come from the rules and the fixture README, not from the macro.
2. A fuzz table: valid timestamps mutated in many ways, checked against an independent
   Python reference parser (datetime does the calendar work).

Run: python tests/test_timestamps.py
"""
import datetime as dt
import random
import re
import sys
import tempfile
from pathlib import Path

import duckdb
import polars as pl

MACROS = Path(__file__).resolve().parent.parent / "pipeline" / "ts_macros.sql"

# 2026-10-03T23:58:10Z, the example in rules.md
EX = 1791071890000
DAY = 86_400_000


def ms(y, mo, d, h=0, mi=0, s=0):
    return int((dt.datetime(y, mo, d, h, mi, s, tzinfo=dt.timezone.utc) - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)).total_seconds()) * 1000


HAND = [
    # --- the three forms, rules.md examples
    ("2026-10-03T23:58:10Z", EX),
    ("2026-10-03T19:58:10-04:00", EX),
    ("1791071890000", EX),
    ("0001791071890000", EX),                     # leading zeros
    ("0", 0),
    ("0000", 0),
    # --- window edges used by the fixtures (2026-10-01 .. 2026-10-15)
    ("2026-09-30T23:59:59Z", ms(2026, 9, 30, 23, 59, 59)),
    ("2026-10-01T00:00:00Z", ms(2026, 10, 1)),
    ("2026-10-15T00:00:00Z", ms(2026, 10, 15)),
    # --- E7: offset converts across midnight
    ("2026-10-04T01:30:00+02:00", ms(2026, 10, 3, 23, 30)),
    # --- E17: offsets
    ("2026-10-03T10:00:00+23:59", ms(2026, 10, 3, 10) - (23 * 60 + 59) * 60000),
    ("2026-10-03T10:00:00-00:00", ms(2026, 10, 3, 10)),
    ("2026-10-03T10:00:00+00:00", ms(2026, 10, 3, 10)),
    ("2026-10-03T10:00:00-23:59", ms(2026, 10, 3, 10) + (23 * 60 + 59) * 60000),
    ("2026-10-03T10:00:00+24:00", None),
    ("2026-10-03T10:00:00+05:60", None),
    ("2026-10-03T10:00:00+0200", None),
    ("2026-10-03T10:00:00+02", None),
    ("2026-10-03T10:00:00+2:00", None),
    ("2026-10-03T10:00:00 +02:00", None),
    ("2026-10-03T10:00:00+02:00:00", None),
    # --- calendar
    ("2028-02-29T12:00:00Z", ms(2028, 2, 29, 12)),            # leap year
    ("2024-02-29T00:00:00Z", ms(2024, 2, 29)),
    ("2000-02-29T00:00:00Z", ms(2000, 2, 29)),                # divisible by 400
    ("2026-02-29T12:00:00Z", None),                           # not a leap year
    ("2100-02-29T12:00:00Z", None),                           # divisible by 100, not 400
    ("2026-02-30T12:00:00Z", None),
    ("2026-04-31T12:00:00Z", None),
    ("2026-06-31T12:00:00Z", None),
    ("2026-09-31T12:00:00Z", None),
    ("2026-11-31T12:00:00Z", None),
    ("2026-01-31T12:00:00Z", ms(2026, 1, 31, 12)),
    ("2026-12-31T23:59:59Z", ms(2026, 12, 31, 23, 59, 59)),
    ("2026-13-01T00:00:00Z", None),                           # month 13
    ("2026-00-10T00:00:00Z", None),
    ("2026-10-00T00:00:00Z", None),
    ("2026-10-32T00:00:00Z", None),
    ("2026-10-03T24:00:00Z", None),                           # hour 24
    ("2026-10-03T23:60:00Z", None),
    ("2026-10-03T23:59:60Z", None),                           # second 60
    ("2026-10-03T23:59:59Z", ms(2026, 10, 3, 23, 59, 59)),
    ("0000-01-01T00:00:00Z", None),                           # year 0 is not a valid date
    ("0001-01-01T00:00:00Z", ms(1, 1, 1)),
    ("9999-12-31T23:59:59Z", ms(9999, 12, 31, 23, 59, 59)),
    ("1970-01-01T00:00:00Z", 0),
    ("1969-12-31T23:59:59Z", -1000),
    # --- wrong shape
    ("", None),
    (" ", None),
    ("2026-10-03T10:00:00z", None),                           # lowercase z
    ("2026-10-03t10:00:00Z", None),                           # lowercase t
    ("2026-10-03 10:00:00Z", None),                           # space for T
    ("2026-10-03 10:00:00", None),
    ("2026-10-03T10:00:00", None),                            # no zone
    ("2026-10-03T10:00:00.000Z", None),                       # fractional seconds
    ("2026-10-03T10:00:00.5Z", None),
    ("2026-10-03T10:00Z", None),
    ("2026-10-03Z", None),
    ("2026-10-3T10:00:00Z", None),
    ("26-10-03T10:00:00Z", None),
    ("20261003T100000Z", None),
    ("2026-10-03T10:00:00Z ", None),                          # trailing space
    (" 2026-10-03T10:00:00Z", None),
    ("2026-10-03T10:00:00Z\r", None),                         # E20: trailing \r
    ("2026-10-03T10:00:00Z\n", None),
    ("1791071890000\r", None),
    ("2026-10-03T10:00:00ZZ", None),
    ("2026-10-03T10:00:00Z+00:00", None),
    ("+2026-10-03T10:00:00Z", None),
    # --- F3 traps
    ("1791071890000.0", None),
    ("1.79e12", None),
    ("-5", None),
    ("+5", None),
    ("1,000", None),
    ("1791071890000 ", None),
    (" 1791071890000", None),
    ("0x10", None),
    ("86400000", DAY),                                        # very short: valid, 1970-01-02
    ("1", 1),
    ("0000000000000000000000000000000001", 1),                # many leading zeros
    ("999999999999999999", 999999999999999999),               # 18 digits
    ("1000000000000000000", None),                            # 19 digits: beyond range, out of any window
    ("9" * 21, None),                                         # E18: 21 digits
    ("9" * 400, None),
    ("0" * 400, 0),
    ("0" * 400 + "1791071890000", EX),
    # --- E18: non-ASCII digits
    ("٨٦٤٠٠٠٠٠", None),                                      # Arabic-Indic digits
    ("١٧٩١٠٧١٨٩٠٠٠٠", None),
    ("２０２６-10-03T10:00:00Z", None),                       # full-width year digit
    ("2026-１0-03T10:00:00Z", None),
    ("2026-10-03T10:00:00-0٤:00", None),
    ("2026-10-03T10:00:00-04:٠٠", None),
    ("１７９１０７１８９０００", None),
]


# ------------------------------------------------------------------ independent reference parser
F12 = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:Z|([+-])([0-9]{2}):([0-9]{2}))", re.ASCII)
EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


def ref_parse(s):
    """Independent re-implementation; datetime does the calendar check."""
    if s is None:
        return None
    if re.fullmatch(r"[0-9]+", s, re.ASCII):
        n = int(s.lstrip("0") or "0")
        return n if len(s.lstrip("0") or "0") <= 18 else None
    m = F12.fullmatch(s)
    if not m:
        return None
    y, mo, d, h, mi, sec = (int(m.group(i)) for i in range(1, 7))
    off = 0
    if m.group(7):
        oh, om = int(m.group(8)), int(m.group(9))
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60 * (1 if m.group(7) == "+" else -1)
    try:
        t = dt.datetime(y, mo, d, h, mi, sec, tzinfo=dt.timezone.utc)   # raises on any impossible value, sec 60 included
    except ValueError:
        return None
    return int((t - EPOCH).total_seconds() - off) * 1000


def fuzz_inputs(n, rng):
    glyphs = list("0123456789-:TZtz+. \r\n\t,\"'") + ["٢", "２", "e", "x", "-", "+", "Z", "٠"]
    out = []
    for _ in range(n):
        kind = rng.random()
        t = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(milliseconds=rng.randrange(0, 4_102_444_800_000))
        if rng.random() < 0.15:                                    # lean on leap days and month ends
            y = rng.choice([1900, 2000, 2024, 2026, 2028, 2100])
            mo = rng.randrange(1, 13)
            d = rng.choice([27, 28, 29, 30, 31])
            t = t.replace(year=y, month=mo, day=1) + dt.timedelta(days=d - 1)
        if kind < 0.4:
            s = t.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif kind < 0.7:
            oh, om = rng.choice([(0, 0), (4, 0), (5, 30), (12, 0), (10, 0), (23, 59), (1, 15)]), 0
            sign = rng.choice("+-")
            hh, mm = oh
            loc = t + dt.timedelta(minutes=(hh * 60 + mm) * (1 if sign == "+" else -1))
            s = loc.strftime("%Y-%m-%dT%H:%M:%S") + f"{sign}{hh:02d}:{mm:02d}"
        else:
            s = str(int(t.timestamp() * 1000))
            if rng.random() < 0.2:
                s = "0" * rng.randrange(1, 30) + s
        for _ in range(rng.choice([0, 0, 1, 1, 2, 3])):            # mutate
            op = rng.randrange(4)
            i = rng.randrange(len(s) + 1)
            if op == 0 and s:
                i = min(i, len(s) - 1)
                s = s[:i] + rng.choice(glyphs) + s[i + 1:]
            elif op == 1:
                s = s[:i] + rng.choice(glyphs) + s[i:]
            elif op == 2 and s:
                i = min(i, len(s) - 1)
                s = s[:i] + s[i + 1:]
            else:
                s = s + rng.choice(["\r", " ", "Z", "0", ".0"])
        out.append(s)
    return out


def main():
    con = duckdb.connect()
    print("duckdb", duckdb.__version__)
    con.execute(MACROS.read_text(encoding="utf-8"))
    failures = 0

    # ---- 1. hand table
    con.execute("CREATE TABLE hand(s VARCHAR, want BIGINT)")
    con.executemany("INSERT INTO hand VALUES (?, ?)", HAND)
    rows = con.execute("SELECT s, want, ts_parse(s) FROM hand").fetchall()
    bad = [(s, w, g) for s, w, g in rows if w != g]
    for s, w, g in bad:
        print(f"FAIL hand {s!r:<45} want {w!r} got {g!r}")
    failures += len(bad)
    print(f"hand table: {len(rows) - len(bad)}/{len(rows)} correct")

    # the reference parser must itself agree with the hand table, or the fuzz proves nothing
    ref_bad = [(s, w, ref_parse(s)) for s, w in HAND if ref_parse(s) != w]
    for s, w, g in ref_bad:
        print(f"FAIL reference vs hand {s!r} want {w!r} got {g!r}")
    failures += len(ref_bad)
    print(f"reference parser vs hand table: {len(HAND) - len(ref_bad)}/{len(HAND)} agree")

    # NULL input
    got = con.execute("SELECT ts_parse(NULL::VARCHAR)").fetchone()[0]
    if got is not None:
        print("FAIL NULL input", got)
        failures += 1

    # ---- 2. fuzz table against the reference
    rng = random.Random(20261120)
    inputs = fuzz_inputs(300_000, rng)
    with tempfile.TemporaryDirectory() as tmp:               # bulk load through Parquet; executemany is far too slow
        path = Path(tmp) / "fuzz.parquet"
        pl.DataFrame({"i": range(len(inputs)), "s": inputs}).write_parquet(path)
        con.execute(f"CREATE TABLE fuzz AS SELECT i, s FROM read_parquet('{path.as_posix()}')")
    got = con.execute("SELECT i, ts_parse(s) FROM fuzz ORDER BY i").fetchall()
    diffs = [(inputs[i], ref_parse(inputs[i]), g) for i, g in got if ref_parse(inputs[i]) != g]
    for s, w, g in diffs[:20]:
        print(f"FAIL fuzz {s!r} reference {w!r} macro {g!r}")
    valid = sum(1 for i, g in got if g is not None)
    failures += len(diffs)
    print(f"fuzz table: {len(got) - len(diffs)}/{len(got)} agree with reference ({valid} valid, {len(got) - valid} rejected)")

    print("\nALL OK" if not failures else f"\n{failures} FAILURES")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
