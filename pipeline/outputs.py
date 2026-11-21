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


def empty_campaign_daily(con):
    """Placeholder until the click stages exist."""
    con.execute("""
        CREATE OR REPLACE TABLE campaign_daily AS
        SELECT NULL::VARCHAR AS campaign_id, NULL::VARCHAR AS channel, NULL::DATE AS day, NULL::BIGINT AS sessions,
               NULL::BIGINT AS purchasing_sessions, NULL::BIGINT AS revenue_cents, NULL::BIGINT AS assisted_sessions
        WHERE false
    """)


def write_parquet(con, table, columns, folder):
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, "part-0.parquet").replace("\\", "/")
    con.execute(f"COPY (SELECT {columns} FROM {table}) TO '{path}' (FORMAT parquet)")
