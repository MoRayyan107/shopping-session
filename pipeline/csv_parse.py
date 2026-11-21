"""
Stage 1: the rules.md CSV dialect, implemented on raw text.

DuckDB's own CSV reader cannot be used: it treats a bare `\\r` as a line ending even with
new_line='\\n' (so `1000\\r` loses its `\\r`), and it has its own ideas about quotes and short rows.
So the files are read as plain text with `read_text` (bytes preserved exactly), split on `\\n` only,
and every line is checked and split in SQL.

Dialect (rules.md "CSV dialect"):
  * Only `\\n` ends a line; `\\r` is an ordinary character of whatever field it is in.
  * The first line of every file is the header and is skipped. A later line equal to the header is data.
  * Fields are separated by every comma (no value contains a comma, quote or newline).
  * A field is bare (no quote at all) or wrapped whole in double quotes; the quotes are syntax.
  * A line with the wrong number of fields, a quote anywhere else, or an empty line is malformed: dropped.
  * An empty field (bare or `""`) is NULL. Spaces are kept.
"""
import glob as _glob

# A well-formed field: no quote at all, or one quote at each end and none inside.
FIELD_RE = '[^"]*|"[^"]*"'


def _unquote(expr):
    """SQL for the value of an already-validated field: strip the wrapping quotes, empty -> NULL."""
    return f"""NULLIF(CASE WHEN starts_with({expr}, '"') THEN substr({expr}, 2, length({expr}) - 2) ELSE {expr} END, '')"""


def load_table(con, table, path_glob, columns):
    """
    Create TABLE `table` with one VARCHAR column per name in `columns`, holding every well-formed
    data line of every file matching `path_glob`. Malformed lines are dropped. Returns the row count.
    """
    ncols = len(columns)
    select_cols = ", ".join(f"{_unquote(f'p[{i + 1}]')} AS {name}" for i, name in enumerate(columns))
    if not _glob.glob(path_glob):                                    # read_text raises on an empty glob
        con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT "
                    + ", ".join(f"NULL::VARCHAR AS {c}" for c in columns) + " WHERE false")
        return 0
    quoted_glob = path_glob.replace("'", "''")
    # Three materialised steps, because DuckDB parallelises a query by chunks of its source rows:
    #   1. one row per file: its lines as a list
    #   2. one row per line (unnest; the header line of each file is dropped)
    #   3. the dialect check and field split, which now reads a many-row table and runs on every thread.
    # Fed straight from the unnest, step 3 ran on one thread whatever the setting (3.3 s vs 0.7 s at 8 threads
    # on 3.2M lines).
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {table}__files AS
        SELECT string_split(content, chr(10)) AS parts FROM read_text('{quoted_glob}')
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {table}__lines AS
        SELECT unnest(parts[2:]) AS line FROM {table}__files
    """)
    con.execute(f"DROP TABLE {table}__files")
    con.execute(f"""
        CREATE OR REPLACE TABLE {table} AS
        WITH split AS (
            SELECT string_split(line, ',') AS p FROM {table}__lines WHERE line <> ''
        )
        SELECT {select_cols}
        FROM split
        WHERE len(p) = {ncols}
          AND len(list_filter(p, x -> NOT regexp_full_match(x, '{FIELD_RE}'))) = 0
    """)
    con.execute(f"DROP TABLE {table}__lines")
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
