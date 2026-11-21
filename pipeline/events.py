"""
Stage 4: events (rules.md "Events, in this order").

  build_events(con, window_start_ms, window_end_ms)
    needs:   raw_events (from csv_parse.load_table), person_map (links.py), products, the ts_* macros
    creates: ev_sessioned(person, ts_ms, session_start, day, event_type, product_id, price_cents, category)
             sessions(person, session_start, day, n_events, revenue_cents, has_purchase)

Order: parse (done) -> drop empty event_id -> dedup on event_id (smallest ingest_ts, BEFORE validation)
-> validate the kept copy -> drop late -> person -> sessionize on distinct timestamps -> day -> category.
"""

DAY_MS = 86_400_000
GAP_MS = 1_800_000
LATE_MS = 86_400_000

PRICE_MACRO = """
-- price_cents of a purchase: ASCII digits only, value 1..10,000,000 (leading zeros allowed), else NULL.
-- The length check comes before the cast, so a very long digit string cannot overflow.
CREATE OR REPLACE MACRO price_parse(s) AS (
  CASE WHEN regexp_full_match(s, '[0-9]+') AND length(ltrim(s, '0')) BETWEEN 1 AND 9
       THEN (CASE WHEN TRY_CAST(ltrim(s, '0') AS BIGINT) BETWEEN 1 AND 10000000 THEN TRY_CAST(ltrim(s, '0') AS BIGINT) END)
  END);
"""


def build_events(con, ws, we, log=lambda *a: None):
    import time

    def step(sql, name=None):
        t = time.time()
        r = con.execute(sql)
        if name:
            log(f"           {time.time() - t:6.2f}s  {name}")
        return r

    con.execute(PRICE_MACRO)

    # 1-2. drop empty event_id, then keep the copy with the smallest ingest_ts (validity is not looked at yet).
    # Copies with equal ingest_ts are identical by the rules; the extra sort keys only make the pick deterministic.
    step("""
        CREATE OR REPLACE TEMP TABLE ev_kept AS
        SELECT * FROM (SELECT *, ts_parse(ingest_ts) AS ingest_ms FROM raw_events WHERE event_id IS NOT NULL)
        QUALIFY row_number() OVER (
            PARTITION BY event_id
            ORDER BY ingest_ms ASC NULLS LAST, user_id, ts, event_type, product_id, price_cents) = 1
    """, 'dedup on event_id (incl. ts_parse(ingest_ts))')
    bad_ingest = con.execute("SELECT count(*) FROM ev_kept WHERE ingest_ms IS NULL").fetchone()[0]
    if bad_ingest:
        log(f"WARNING: {bad_ingest} kept events have an unparseable ingest_ts (rules say this never happens); they are dropped as late")

    # 3-4. validate the kept copy, then drop late events
    step(f"""
        CREATE OR REPLACE TEMP TABLE ev_valid AS
        SELECT user_id, ts_ms, event_type, product_id,
               CASE WHEN event_type = 'purchase' THEN price END AS price_cents
        FROM (SELECT *, ts_parse(ts) AS ts_ms, price_parse(price_cents) AS price FROM ev_kept)
        WHERE user_id IS NOT NULL
          AND product_id IS NOT NULL
          AND ts_ms >= {ws} AND ts_ms < {we}
          AND event_type IN ('view', 'cart', 'purchase')
          AND (event_type <> 'purchase' OR price IS NOT NULL)
          AND ingest_ms - ts_ms <= {LATE_MS}
    """, 'validate + late (ts_parse(ts), price)')

    # 5. person
    step("""
        CREATE OR REPLACE TEMP TABLE ev_person AS
        SELECT COALESCE(m.person, e.user_id) AS person, e.ts_ms, e.event_type, e.product_id, e.price_cents
        FROM ev_valid e LEFT JOIN person_map m ON m.id = e.user_id
    """, 'map to person')

    # 6. sessions on DISTINCT timestamps, so equal timestamps always share a session whatever the read order
    step(f"""
        CREATE OR REPLACE TEMP TABLE sess_ts AS
        SELECT person, ts_ms, min(ts_ms) OVER (PARTITION BY person, sess_no) AS session_start
        FROM (
            SELECT person, ts_ms,
                   sum(is_new) OVER (PARTITION BY person ORDER BY ts_ms ROWS UNBOUNDED PRECEDING) AS sess_no
            FROM (
                SELECT person, ts_ms,
                       CASE WHEN ts_ms - lag(ts_ms) OVER (PARTITION BY person ORDER BY ts_ms) <= {GAP_MS}
                            THEN 0 ELSE 1 END AS is_new
                FROM (SELECT DISTINCT person, ts_ms FROM ev_person)
            )
        )
    """, 'sessionize (distinct ts, lag, running sum)')

    # 7-8. day of the session start (exact integer maths) and category
    step(f"""
        CREATE OR REPLACE TABLE ev_sessioned AS
        SELECT e.person, e.ts_ms, s.session_start,
               DATE '1970-01-01' + CAST(s.session_start // {DAY_MS} AS INTEGER) AS day,
               e.event_type, e.product_id, e.price_cents,
               COALESCE(p.category, 'unknown') AS category
        FROM ev_person e
        JOIN sess_ts s USING (person, ts_ms)
        LEFT JOIN products p ON p.product_id = e.product_id
    """, 'day + category join')
    step("""
        CREATE OR REPLACE TABLE sessions AS
        SELECT person, session_start, any_value(day) AS day,
               count(*) AS n_events,
               CAST(COALESCE(sum(price_cents) FILTER (WHERE event_type = 'purchase'), 0) AS BIGINT) AS revenue_cents,
               bool_or(event_type = 'purchase') AS has_purchase
        FROM ev_sessioned GROUP BY person, session_start
    """, 'sessions table')
    return {
        "events_kept": con.execute("SELECT count(*) FROM ev_kept").fetchone()[0],
        "events_valid": con.execute("SELECT count(*) FROM ev_valid").fetchone()[0],
        "sessions": con.execute("SELECT count(*) FROM sessions").fetchone()[0],
    }
