#!/usr/bin/env python3
"""
Shopping Sessions: DuckDB pipeline.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)

Steps (see rules.md):
  1. Read every CSV as raw lines and parse them ourselves, so the CSV dialect
     (quotes, empty = null, spaces kept, '\r' is data) matches the rules exactly.
  2. Links -> connected components (SciPy) -> each ID's person name.
  3. Events: dedup -> validate -> drop late -> sessionize per person.
  4. Clicks: dedup -> validate -> credit (ASOF join) and assists (interval join).
  5. Aggregate and write both Parquet tables.
"""
import json
import os
import sys
from datetime import datetime, timezone

import duckdb
import numpy as np
import pyarrow as pa
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

DATA = sys.argv[1] if len(sys.argv) > 2 else os.environ.get("DATA_DIR", "/data")
OUT = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 7 * DAY_MS

con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.75))}GB'")
os.makedirs(SCRATCH, exist_ok=True)
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")

if os.environ.get("PROFILE"):
    import time as _t
    _raw_exec = con.execute

    class _Timed:
        def __getattr__(self, k):
            return getattr(con_real, k)

        def execute(self, q, *a):
            t0 = _t.perf_counter()
            r = _raw_exec(q, *a)
            print(f"{_t.perf_counter() - t0:7.2f}s  {' '.join(q.split())[:70]}", file=sys.stderr)
            return r

    con_real, con = con, _Timed()


def iso_ms(s):
    return int(datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp() * 1000)


with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)
WS = iso_ms(manifest["window_start"])
WE = iso_ms(manifest["window_end"])

# ---------------------------------------------------------------- SQL helpers
# A field is either unquoted with no quote/comma inside, or "..." around the whole field.
FIELD = '("[^",]*"|[^",]*)'

con.execute(r"""
CREATE MACRO fv(f) AS nullif(CASE WHEN starts_with(f, '"') THEN f[2:-2] ELSE f END, '');

-- Timestamp in F1 / F2 / F3 -> epoch ms, or NULL when it is a bad timestamp.
-- The TIMESTAMP cast rejects impossible dates, minute/second 60, but accepts hour 24, so check that.
CREATE MACRO ts_ms(s) AS CASE
  WHEN regexp_full_match(s, '[0-9]+') THEN
    CASE WHEN length(ltrim(s, '0')) <= 15 THEN TRY_CAST(s AS BIGINT) END
  WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})')
       AND s[12:13] <= '23' THEN
    epoch_ms(TRY_CAST(s[1:19] AS TIMESTAMP))
    - CASE WHEN length(s) = 20 THEN 0
           WHEN s[21:22] <= '23' AND s[24:25] <= '59' THEN
             (CASE WHEN s[20] = '-' THEN -1 ELSE 1 END)
             * (TRY_CAST(s[21:22] AS BIGINT) * 3600000 + TRY_CAST(s[24:25] AS BIGINT) * 60000)
      END
END;
-- ingest_ts is always valid F1.
CREATE MACRO f1_ms(s) AS epoch_ms(CAST(s[1:19] AS TIMESTAMP));
""")


def raw_lines(folder, fast):
    """Every data line (header skipped per file) of every *.csv in folder, as a relation `line`."""
    glob = os.path.join(DATA, folder, "*.csv")
    if fast:
        # DuckDB's reader with no quoting and a delimiter that never occurs: one column per line.
        # In strict mode it raises on a '\r', and then we take the exact path below.
        return f"""SELECT line FROM read_csv('{glob}', columns={{'line': 'VARCHAR'}}, header=true,
                     delim='\x01', quote='', escape='', new_line='\\n', auto_detect=false,
                     strict_mode=true)"""
    # Slow but exact: split each file on '\n' ourselves.
    return f"""
      WITH t AS (SELECT string_split(content, chr(10)) AS ls FROM read_text('{glob}'))
      SELECT CASE WHEN ls[i] = '' THEN NULL ELSE ls[i] END AS line
      FROM (SELECT ls, unnest(range(2, len(ls) + 1)) AS i FROM t)
      WHERE NOT (i = len(ls) AND ls[i] = '')
    """


def parsed(folder, cols, fast):
    n = len(cols)
    pattern = "^" + FIELD + ("(," + FIELD + "){%d}" % (n - 1) if n > 1 else "") + "$"
    sel = ", ".join(f"fv(p[{i + 1}]) AS {c}" for i, c in enumerate(cols))
    return f"""
      SELECT {sel} FROM (
        SELECT string_split(line, ',') AS p FROM ({raw_lines(folder, fast)})
        WHERE line IS NOT NULL AND regexp_full_match(line, '{pattern}')
      )
    """


def create_from_csv(make_sql, folder, cols):
    """Run make_sql(parsed_relation), first with the fast reader, then with the exact one."""
    try:
        con.execute(make_sql(parsed(folder, cols, True)))
    except duckdb.Error:
        con.execute(make_sql(parsed(folder, cols, False)))


def small_table(name, cols):
    """products.csv / campaigns.csv: clean, but parse them the same way."""
    path = os.path.join(DATA, name)
    n = len(cols)
    pattern = "^" + FIELD + "(," + FIELD + "){%d}$" % (n - 1)
    sel = ", ".join(f"fv(p[{i + 1}]) AS {c}" for i, c in enumerate(cols))
    return f"""
      SELECT {sel} FROM (
        SELECT string_split(line, ',') AS p FROM read_csv('{path}', columns={{'line': 'VARCHAR'}},
            header=true, delim='\x01', quote='', escape='', new_line='\\n', auto_detect=false)
        WHERE line IS NOT NULL AND regexp_full_match(line, '{pattern}'))
    """


con.execute(f"CREATE TABLE products AS SELECT DISTINCT product_id, category FROM ({small_table('products.csv', ['product_id', 'category'])})")
con.execute(f"CREATE TABLE campaigns AS SELECT DISTINCT campaign_id, channel FROM ({small_table('campaigns.csv', ['campaign_id', 'channel'])})")

# ---------------------------------------------------------------- 1. Linked IDs
create_from_csv(lambda src: f"""
CREATE OR REPLACE TABLE links AS
SELECT a, b FROM ({src}) WHERE a IS NOT NULL AND b IS NOT NULL
""", "links", ["a", "b"])
# Number the IDs in byte-wise order, so the smallest ID of a component is its smallest index.
con.execute("""
CREATE TABLE ids AS
SELECT id, (row_number() OVER (ORDER BY id) - 1)::INTEGER AS ix
FROM (SELECT a AS id FROM links UNION SELECT b FROM links)
""")
n_ids = con.execute("SELECT count(*) FROM ids").fetchone()[0]
if n_ids:
    edges = con.execute("""
        SELECT ia.ix AS x, ib.ix AS y FROM links
        JOIN ids ia ON ia.id = links.a JOIN ids ib ON ib.id = links.b
    """).fetchnumpy()
    x, y = edges["x"].astype(np.int32), edges["y"].astype(np.int32)
    g = coo_matrix((np.ones(len(x), dtype=np.int8), (x, y)), shape=(n_ids, n_ids)).tocsr()
    n_comp, labels = connected_components(g, directed=False)
    smallest = np.full(n_comp, n_ids, dtype=np.int64)
    np.minimum.at(smallest, labels, np.arange(n_ids))
    person_ix = pa.table({"ix": np.arange(n_ids, dtype=np.int32),
                          "pix": smallest[labels].astype(np.int32)})
    con.register("person_ix", person_ix)
    con.execute("""
    CREATE TABLE person_map AS
    SELECT a.id, b.id AS person FROM person_ix p JOIN ids a ON a.ix = p.ix JOIN ids b ON b.ix = p.pix
    """)
else:
    con.execute("CREATE TABLE person_map (id VARCHAR, person VARCHAR)")

# ---------------------------------------------------------------- 2. Events
# Parse and validate every copy right away into compact columns (validity is a per-copy
# property), then dedup: the copy with the smallest ingest_ts is kept, valid or not.
create_from_csv(lambda src: f"""
CREATE OR REPLACE TABLE ev_raw AS
SELECT event_id, user_id, ts, ingest, et, product_id, price,
       (user_id IS NOT NULL AND ts >= {WS} AND ts < {WE} AND et IS NOT NULL AND has_product
        AND (et <> 2 OR price IS NOT NULL)) AS ok
FROM (
  SELECT event_id, user_id, ts_ms(ts) AS ts, f1_ms(ingest_ts) AS ingest,
         CASE event_type WHEN 'view' THEN 0 WHEN 'cart' THEN 1 WHEN 'purchase' THEN 2 END::UTINYINT AS et,
         product_id IS NOT NULL AS has_product,
         CASE WHEN event_type = 'purchase' THEN product_id END AS product_id,
         CASE WHEN event_type = 'purchase' AND regexp_full_match(price_cents, '[0-9]+')
                   AND length(ltrim(price_cents, '0')) <= 8
                   AND TRY_CAST(price_cents AS BIGINT) BETWEEN 1 AND 10000000
              THEN TRY_CAST(price_cents AS BIGINT) END AS price
  FROM ({src}) WHERE event_id IS NOT NULL
)
""", "events", ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"])
con.execute(f"""
CREATE TABLE events AS
WITH kept AS (
  SELECT arg_min(struct_pack(user_id, ts, ingest, et, product_id, price, ok), ingest) AS r
  FROM ev_raw GROUP BY event_id
)
SELECT coalesce(pm.person, r.user_id) AS person, r.ts AS ts, r.et AS et, r.product_id AS product_id,
       CASE WHEN r.et = 2 THEN r.price END AS price
FROM kept LEFT JOIN person_map pm ON pm.id = r.user_id
WHERE r.ok AND r.ingest - r.ts <= {DAY_MS}
""")
con.execute("DROP TABLE ev_raw")

# Sessionize: new session when the gap to the previous event (by ts) is over 30 min.
# Equal timestamps have gap 0, so they always share a session.
con.execute(f"""
CREATE TABLE ev_s AS
SELECT person, ts, et, product_id, price,
       sum(new) OVER (PARTITION BY person ORDER BY ts ROWS UNBOUNDED PRECEDING) AS sid
FROM (
  SELECT *, CASE WHEN ts - lag(ts) OVER (PARTITION BY person ORDER BY ts) > {GAP_MS} THEN 1 ELSE 0 END AS new
  FROM events
)
""")
con.execute("DROP TABLE events")
con.execute(f"""
CREATE TABLE sessions AS
SELECT person, sid, min(ts) AS start,
       DATE '1970-01-01' + CAST(min(ts) // {DAY_MS} AS INTEGER) AS day,
       count(*) AS n_events,
       coalesce(sum(price), 0) AS revenue,
       count(price) > 0 AS purchased
FROM ev_s GROUP BY person, sid
""")
con.execute("""
CREATE TABLE cat_rev AS
SELECT s.person, s.day, coalesce(p.category, 'unknown') AS category, sum(e.price) AS rev
FROM ev_s e JOIN sessions s USING (person, sid)
LEFT JOIN products p ON p.product_id = e.product_id
WHERE e.price IS NOT NULL
GROUP BY ALL
""")
con.execute("DROP TABLE ev_s")

# ---------------------------------------------------------------- 3. Clicks
create_from_csv(lambda src: f"""
CREATE OR REPLACE TABLE clicks_raw AS
SELECT DISTINCT ON (click_id) click_id, user_id, campaign_id, ts FROM ({src}) WHERE click_id IS NOT NULL
""", "clicks", ["click_id", "user_id", "campaign_id", "ts"])
con.execute(f"""
CREATE TABLE clicks AS
WITH c AS (SELECT * FROM clicks_raw), v AS (SELECT click_id, user_id, campaign_id, ts_ms(ts) AS ts FROM c)
SELECT coalesce(pm.person, v.user_id) AS person, v.click_id, v.campaign_id, v.ts
FROM v LEFT JOIN person_map pm ON pm.id = v.user_id
WHERE v.user_id IS NOT NULL AND v.ts IS NOT NULL
  AND v.ts >= {WS - WEEK_MS} AND v.ts < {WE}
  AND v.campaign_id IN (SELECT campaign_id FROM campaigns)
""")

# Credit: latest click at or before the start; ties -> smallest click_id.
con.execute(f"""
CREATE TABLE credit AS
WITH c1 AS (
  SELECT person, ts, arg_min(campaign_id, click_id) AS campaign_id FROM clicks GROUP BY person, ts
)
SELECT s.person, s.sid,
       CASE WHEN c1.ts IS NOT NULL AND s.start - c1.ts <= {WEEK_MS} THEN c1.campaign_id ELSE 'organic' END AS campaign_id
FROM sessions s ASOF LEFT JOIN c1 ON s.person = c1.person AND c1.ts <= s.start
""")

# Assists: per (person, campaign) merge click coverage intervals [ts, ts + 7d];
# a session is assisted when its start falls in one. Bucketed by day to keep the join small.
con.execute(f"""
CREATE TABLE assists AS
WITH c AS (SELECT DISTINCT person, campaign_id, ts FROM clicks),
g AS (
  SELECT *, sum(brk) OVER (PARTITION BY person, campaign_id ORDER BY ts ROWS UNBOUNDED PRECEDING) AS grp
  FROM (SELECT *, CASE WHEN ts - lag(ts) OVER (PARTITION BY person, campaign_id ORDER BY ts) <= {WEEK_MS}
                       THEN 0 ELSE 1 END AS brk FROM c)
),
iv AS (SELECT person, campaign_id, min(ts) AS lo, max(ts) + {WEEK_MS} AS hi FROM g GROUP BY person, campaign_id, grp),
ivd AS (
  SELECT person, campaign_id, lo, hi, unnest(range(lo // {DAY_MS}, hi // {DAY_MS} + 1)) AS dnum FROM iv
)
SELECT s.person, s.sid, s.day, ivd.campaign_id
FROM sessions s JOIN ivd
  ON ivd.person = s.person AND ivd.dnum = s.start // {DAY_MS}
 AND s.start >= ivd.lo AND s.start <= ivd.hi
""")

# ---------------------------------------------------------------- 4. Outputs
os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)

con.execute(f"""
COPY (
  WITH top AS (
    SELECT person, day, arg_min(category, struct_pack(neg := -rev, category)) AS top_category
    FROM cat_rev GROUP BY person, day
  ),
  ud AS (
    SELECT person, day, count(*) AS sessions, sum(n_events) AS events, sum(revenue) AS revenue_cents
    FROM sessions GROUP BY person, day
  )
  SELECT ud.person AS user_id, ud.day, ud.sessions::BIGINT AS sessions, ud.events::BIGINT AS events,
         ud.revenue_cents::BIGINT AS revenue_cents, top.top_category
  FROM ud LEFT JOIN top USING (person, day)
) TO '{os.path.join(OUT, "user_daily", "part-0.parquet")}' (FORMAT parquet)
""")

con.execute(f"""
COPY (
  WITH cr AS (
    SELECT c.campaign_id, s.day, count(*) AS sessions, count(*) FILTER (WHERE s.purchased) AS purchasing_sessions,
           sum(s.revenue) AS revenue_cents
    FROM credit c JOIN sessions s USING (person, sid) GROUP BY ALL
  ),
  asd AS (SELECT campaign_id, day, count(*) AS assisted_sessions FROM assists GROUP BY ALL)
  SELECT k.campaign_id,
         CASE WHEN k.campaign_id = 'organic' THEN 'organic' ELSE cmp.channel END AS channel,
         k.day,
         coalesce(cr.sessions, 0)::BIGINT AS sessions,
         coalesce(cr.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
         coalesce(cr.revenue_cents, 0)::BIGINT AS revenue_cents,
         coalesce(asd.assisted_sessions, 0)::BIGINT AS assisted_sessions
  FROM (SELECT campaign_id, day FROM cr UNION SELECT campaign_id, day FROM asd) k
  LEFT JOIN cr USING (campaign_id, day)
  LEFT JOIN asd USING (campaign_id, day)
  LEFT JOIN campaigns cmp ON cmp.campaign_id = k.campaign_id
) TO '{os.path.join(OUT, "campaign_daily", "part-0.parquet")}' (FORMAT parquet)
""")
print("done")
