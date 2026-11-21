"""
Output tables (rules.md "Outputs").

  build_user_daily(con)       needs ev_sessioned, sessions; creates user_daily
  write_parquet(con, table, folder)
"""
import os

USER_DAILY_COLUMNS = "user_id, day, sessions, events, revenue_cents, top_category"
CAMPAIGN_DAILY_COLUMNS = "campaign_id, channel, day, sessions, purchasing_sessions, revenue_cents, assisted_sessions"


def build_user_daily(con):
    con.execute("""
        CREATE OR REPLACE TABLE user_daily AS
        WITH per_day AS (
            SELECT person, day, count(*) AS sessions, sum(n_events) AS events, sum(revenue_cents) AS revenue_cents
            FROM sessions GROUP BY person, day
        ),
        top AS (                                  -- highest summed purchase price; ties -> byte-wise smallest
            SELECT person, day, category FROM (
                SELECT person, day, category, sum(price_cents) AS amount
                FROM ev_sessioned WHERE event_type = 'purchase'
                GROUP BY person, day, category
            )
            QUALIFY row_number() OVER (PARTITION BY person, day ORDER BY amount DESC, category ASC) = 1
        )
        SELECT d.person AS user_id, d.day,
               CAST(d.sessions AS BIGINT) AS sessions, CAST(d.events AS BIGINT) AS events,
               CAST(d.revenue_cents AS BIGINT) AS revenue_cents, t.category AS top_category
        FROM per_day d LEFT JOIN top t USING (person, day)
    """)
    return con.execute("SELECT count(*) FROM user_daily").fetchone()[0]


def build_campaign_daily(con):
    """Needs session_credit, session_assist, campaigns. One row per (campaign, day) credited or assisted."""
    con.execute("""
        CREATE OR REPLACE TABLE campaign_daily AS
        WITH credited AS (
            SELECT campaign_id, day, count(*) AS sessions,
                   count(*) FILTER (WHERE has_purchase) AS purchasing_sessions,
                   sum(revenue_cents) AS revenue_cents
            FROM session_credit GROUP BY campaign_id, day
        ),
        assisted AS (
            SELECT campaign_id, day, count(*) AS assisted_sessions FROM session_assist GROUP BY campaign_id, day
        )
        SELECT campaign_id,
               CASE WHEN campaign_id = 'organic' THEN 'organic' ELSE k.channel END AS channel,
               day,
               CAST(COALESCE(c.sessions, 0) AS BIGINT) AS sessions,
               CAST(COALESCE(c.purchasing_sessions, 0) AS BIGINT) AS purchasing_sessions,
               CAST(COALESCE(c.revenue_cents, 0) AS BIGINT) AS revenue_cents,
               CAST(COALESCE(a.assisted_sessions, 0) AS BIGINT) AS assisted_sessions
        FROM credited c FULL OUTER JOIN assisted a USING (campaign_id, day)
        LEFT JOIN campaigns k USING (campaign_id)
    """)
    return con.execute("SELECT count(*) FROM campaign_daily").fetchone()[0]


def self_checks(con, log):
    """rules.md's self-check: equal session and revenue sums, assists >= credited non-organic sessions."""
    us, ur = con.execute("SELECT sum(sessions), sum(revenue_cents) FROM user_daily").fetchone()
    cs, cr, ca, cn = con.execute("""
        SELECT sum(sessions), sum(revenue_cents), sum(assisted_sessions),
               sum(sessions) FILTER (WHERE campaign_id <> 'organic') FROM campaign_daily""").fetchone()
    problems = []
    if (us or 0) != (cs or 0):
        problems.append(f"sessions {us} vs {cs}")
    if (ur or 0) != (cr or 0):
        problems.append(f"revenue {ur} vs {cr}")
    if (ca or 0) < (cn or 0):
        problems.append(f"assisted {ca} < credited non-organic {cn}")
    if problems:
        log("WARNING: self-check failed: " + "; ".join(problems))
    return "ok" if not problems else "FAILED: " + "; ".join(problems)


def write_parquet(con, table, columns, folder):
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, "part-0.parquet").replace("\\", "/")
    con.execute(f"COPY (SELECT {columns} FROM {table}) TO '{path}' (FORMAT parquet)")
