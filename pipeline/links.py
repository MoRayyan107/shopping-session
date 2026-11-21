"""
Stage 3: linked IDs -> persons (rules.md "Linked IDs").

  build_person_map(con, links_table) creates TABLE person_map(id, person):
    for every ID that is linked to a *different* ID, the person's name (the byte-wise smallest ID of
    its component), omitting rows where id = person. Every other ID is its own person, so callers use

        LEFT JOIN person_map m ON m.id = x.user_id   ...   COALESCE(m.person, x.user_id)

Steps: clean + dedup the edges in SQL, number the IDs, run union-find on integers in Python (iterative,
path compression, union by size: near-linear for 400-link chains and thousand-ID hubs alike), send the
(idx, root) pairs back into DuckDB, and name each component with SQL min(), which DuckDB evaluates
byte-wise (tests/test_byte_order.py). Python never compares ID strings.
"""
import os
import tempfile


def union_find(n, edges):
    """Component root for every node 0..n-1. `edges` is an iterable of (a, b) integer pairs."""
    parent = list(range(n))
    size = [1] * n

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]          # path halving
            x = parent[x]
        return x

    for a, b in edges:
        ra, rb = find(a), find(b)
        if ra != rb:
            if size[ra] < size[rb]:
                ra, rb = rb, ra
            parent[rb] = ra
            size[ra] += size[rb]
    return [find(i) for i in range(n)]


def components_to_table(con, table, roots, tmp_dir=None):
    """
    Send roots (roots[i] = root of node i) into DuckDB as TABLE `table`(idx INTEGER, root INTEGER).

    Goes through a temporary CSV file. Do NOT pass the lists as query parameters: binding a Python list
    costs about 0.4 ms per element and grows faster than linearly (10,000 entries took 8 s, a million
    never finished); the file route takes about 0.4 s per million.
    """
    if not roots:                                            # an empty file cannot be read back
        con.execute(f"CREATE OR REPLACE TEMP TABLE {table} AS SELECT NULL::INTEGER AS idx, NULL::INTEGER AS root WHERE false")
        return
    fd, path = tempfile.mkstemp(suffix=".csv", dir=tmp_dir)
    try:
        with os.fdopen(fd, "w") as f:
            f.write("".join(f"{i},{r}\n" for i, r in enumerate(roots)))
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {table} AS "
            f"SELECT idx, root FROM read_csv('{path.replace(chr(92), '/')}', header=false, auto_detect=false, "
            "delim=',', columns={'idx': 'INTEGER', 'root': 'INTEGER'})"
        )
    finally:
        os.remove(path)


def build_person_map(con, links_table="links", tmp_dir=None):
    """Create TABLE person_map(id, person) from links_table(user_id_a, user_id_b). Returns its row count."""
    # 1. usable edges: both sides present, not a self-link, each pair once whichever way round
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE link_edges AS
        SELECT DISTINCT least(user_id_a, user_id_b) AS a, greatest(user_id_a, user_id_b) AS b
        FROM {links_table}
        WHERE user_id_a IS NOT NULL AND user_id_b IS NOT NULL AND user_id_a <> user_id_b
    """)
    # 2. number the IDs 0..N-1
    con.execute("""
        CREATE OR REPLACE TEMP TABLE link_ids AS
        SELECT id, (row_number() OVER ())::INTEGER - 1 AS idx
        FROM (SELECT a AS id FROM link_edges UNION SELECT b FROM link_edges)
    """)
    n = con.execute("SELECT count(*) FROM link_ids").fetchone()[0]
    # 3. components, in Python on integers
    edges = con.execute("""
        SELECT ia.idx, ib.idx FROM link_edges e
        JOIN link_ids ia ON ia.id = e.a JOIN link_ids ib ON ib.id = e.b
    """).fetchall()
    components_to_table(con, "link_comp", union_find(n, edges), tmp_dir)
    # 4. the person is the byte-wise smallest ID of the component; unchanged IDs need no row
    con.execute("""
        CREATE OR REPLACE TABLE person_map AS
        SELECT id, person FROM (
            SELECT i.id, min(i.id) OVER (PARTITION BY c.root) AS person
            FROM link_ids i JOIN link_comp c USING (idx)
        ) WHERE id <> person
    """)
    return con.execute("SELECT count(*) FROM person_map").fetchone()[0]
