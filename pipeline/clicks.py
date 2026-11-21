"""
Stages 6-7: clicks, credit and assists (rules.md "Clicks").

  build_clicks(con, window_start_ms, window_end_ms)
    needs:   raw_clicks (csv_parse.load_table), campaigns, person_map, sessions, the ts_* macros
    creates: ck_valid(person, click_id, campaign_id, ts_ms)
             session_credit(person, session_start, day, has_purchase, revenue_cents, campaign_id)
             session_assist(person, session_start, day, campaign_id)   one row per assisting campaign
"""
import time

WEEK_MS = 604_800_000


def build_clicks(con, ws, we, log=lambda *a: None):
    def step(sql, name):
        t = time.time()
        con.execute(sql)
        log(f"           {time.time() - t:6.2f}s  {name}")

    # 1-2. drop empty click_id; dedup (copies are identical by the rules, so the pick only has to be deterministic)
    step("""
        CREATE OR REPLACE TEMP TABLE ck_kept AS
        SELECT * FROM raw_clicks WHERE click_id IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY click_id ORDER BY user_id, campaign_id, ts) = 1
    """, "dedup on click_id")
    differing = con.execute("""
        SELECT count(*) FROM (
            SELECT click_id FROM raw_clicks WHERE click_id IS NOT NULL
            GROUP BY click_id HAVING count(DISTINCT row(user_id, campaign_id, ts)) > 1)
    """).fetchone()[0]
    if differing:
        log(f"WARNING: {differing} click_ids have copies with different values (rules say copies are identical)")

    # 3. validate: user present, ts valid and in the click window, campaign known (exact match); then the person
    step(f"""
        CREATE OR REPLACE TEMP TABLE ck_valid AS
        SELECT COALESCE(m.person, c.user_id) AS person, c.click_id, c.campaign_id, c.ts_ms
        FROM (SELECT *, ts_parse(ts) AS ts_ms FROM ck_kept) c
        JOIN campaigns k ON k.campaign_id = c.campaign_id
        LEFT JOIN person_map m ON m.id = c.user_id
        WHERE c.user_id IS NOT NULL AND c.ts_ms >= {ws - WEEK_MS} AND c.ts_ms < {we}
    """, "validate clicks + person")

    # 4-5. credit: per person and exact millisecond keep only the smallest click_id (the tie rule), then take the
    # latest such click at or before the session start; older than 7 days, or none at all, is organic
    step(f"""
        CREATE OR REPLACE TABLE session_credit AS
        WITH best AS (
            SELECT person, ts_ms, arg_min(campaign_id, click_id) AS campaign_id
            FROM ck_valid GROUP BY person, ts_ms
        )
        SELECT s.person, s.session_start, s.day, s.has_purchase, s.revenue_cents,
               CASE WHEN b.ts_ms IS NOT NULL AND s.session_start - b.ts_ms <= {WEEK_MS}
                    THEN b.campaign_id ELSE 'organic' END AS campaign_id
        FROM sessions s ASOF LEFT JOIN best b ON s.person = b.person AND s.session_start >= b.ts_ms
    """, "credit (ASOF join)")

    # 7. assists: every campaign with a click in [start - 7 days, start], once per session
    step(f"""
        CREATE OR REPLACE TABLE session_assist AS
        SELECT DISTINCT s.person, s.session_start, s.day, c.campaign_id
        FROM sessions s
        JOIN (SELECT DISTINCT person, campaign_id, ts_ms FROM ck_valid) c
          ON c.person = s.person AND c.ts_ms BETWEEN s.session_start - {WEEK_MS} AND s.session_start
    """, "assists (range join)")

    return {
        "clicks_kept": con.execute("SELECT count(*) FROM ck_kept").fetchone()[0],
        "clicks_valid": con.execute("SELECT count(*) FROM ck_valid").fetchone()[0],
        "credited_non_organic": con.execute("SELECT count(*) FROM session_credit WHERE campaign_id <> 'organic'").fetchone()[0],
        "assists": con.execute("SELECT count(*) FROM session_assist").fetchone()[0],
    }
