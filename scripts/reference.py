#!/usr/bin/env python3
"""Slow, literal reference implementation of rules.md, written independently of both pipelines.
Used only for cross-checking. Usage: reference.py DATA_DIR OUT_DIR"""
import calendar, glob, json, os, re, sys
from collections import defaultdict
import duckdb

DATA, OUT = sys.argv[1], sys.argv[2]
DAY, GAP, WEEK = 86_400_000, 1_800_000, 7 * 86_400_000
DIG = re.compile(r"[0-9]+\Z")
F12 = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(Z|([+-])([0-9]{2}):([0-9]{2}))\Z")


def lines(folder):
    for f in sorted(glob.glob(os.path.join(DATA, folder, "*.csv"))):
        with open(f, "rb") as fh:
            ls = fh.read().decode().split("\n")
        if ls[-1] == "":
            ls.pop()
        yield from ls[1:]


def parse(line, n):
    if line == "":
        return None
    parts = line.split(",")
    if len(parts) != n:
        return None
    out = []
    for p in parts:
        if '"' in p:
            if len(p) >= 2 and p[0] == '"' and p[-1] == '"' and '"' not in p[1:-1]:
                p = p[1:-1]
            else:
                return None
        out.append(p if p != "" else None)
    return out


def ts_ms(s):
    if s is None:
        return None
    if DIG.match(s):
        v = int(s)
        return v if v < 2**63 else None
    m = F12.match(s)
    if not m:
        return None
    y, mo, d, h, mi, se = (int(m.group(i)) for i in range(1, 7))
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or se > 59:
        return None
    if y < 1 or d < 1 or d > calendar.monthrange(y, mo)[1]:
        return None
    off = 0
    if m.group(7) != "Z":
        oh, om = int(m.group(9)), int(m.group(10))
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60000 * (-1 if m.group(8) == "-" else 1)
    days = (calendar.timegm((y, mo, d, 0, 0, 0)) // 86400)
    return days * DAY + h * 3600000 + mi * 60000 + se * 1000 - off


man = json.load(open(os.path.join(DATA, "manifest.json")))
WS, WE = ts_ms(man["window_start"]), ts_ms(man["window_end"])


def small(name, n):
    with open(os.path.join(DATA, name)) as fh:
        ls = fh.read().split("\n")[1:]
    return [p for p in (parse(l, n) for l in ls if l) if p]


products = {p[0]: p[1] for p in small("products.csv", 2)}
campaigns = {c[0]: c[1] for c in small("campaigns.csv", 2)}

# links: union-find
parent = {}


def find(x):
    root = x
    while parent.get(root, root) != root:
        root = parent[root]
    while parent.get(x, x) != root:
        parent[x], x = root, parent[x]
    return root


for l in lines("links"):
    p = parse(l, 2)
    if not p or p[0] is None or p[1] is None:
        continue
    a, b = find(p[0]), find(p[1])
    parent.setdefault(a, a); parent.setdefault(b, b)
    if a != b:
        if a < b:
            parent[b] = a
        else:
            parent[a] = b
name = {}
for x in list(parent):
    r = find(x)
    if r not in name or x < name[r]:
        name[r] = x


def person(uid):
    if uid in parent:
        return name[find(uid)]
    return uid


# events
kept = {}
for l in lines("events"):
    p = parse(l, 7)
    if not p or p[0] is None:
        continue
    ing = p[3]
    cur = kept.get(p[0])
    if cur is None or ing < cur[3]:
        kept[p[0]] = p
by_person = defaultdict(list)
for eid, (e, u, ts, ing, et, prod, price) in kept.items():
    t = ts_ms(ts)
    if u is None or t is None or not (WS <= t < WE) or et not in ("view", "cart", "purchase") or prod is None:
        continue
    pr = None
    if et == "purchase":
        if price is None or not DIG.match(price) or not (1 <= int(price) <= 10_000_000):
            continue
        pr = int(price)
    if ts_ms(ing) - t > DAY:
        continue
    by_person[person(u)].append((t, pr, prod))
del kept

clicks_by = defaultdict(list)
seen = set()
for l in lines("clicks"):
    p = parse(l, 4)
    if not p or p[0] is None or p[0] in seen:
        continue
    seen.add(p[0])
    k, u, c, ts = p
    t = ts_ms(ts)
    if u is None or t is None or not (WS - WEEK <= t < WE) or c not in campaigns:
        continue
    clicks_by[person(u)].append((t, k, c))
for v in clicks_by.values():
    v.sort()

ud = {}
cd = defaultdict(lambda: [0, 0, 0, 0])
import bisect
for per, evs in by_person.items():
    evs.sort(key=lambda x: x[0])
    sessions = []
    prev = None
    for e in evs:
        if prev is None or e[0] - prev > GAP:
            sessions.append([])
        sessions[-1].append(e)
        prev = e[0]
    cl = clicks_by.get(per, [])
    cts = [c[0] for c in cl]
    for s in sessions:
        start = s[0][0]
        day = start // DAY
        rev = sum(e[1] for e in s if e[1] is not None)
        purch = any(e[1] is not None for e in s)
        r = ud.setdefault((per, day), [0, 0, 0, defaultdict(int)])
        r[0] += 1; r[1] += len(s); r[2] += rev
        for e in s:
            if e[1] is not None:
                r[3][products.get(e[2], "unknown")] += e[1]
        lo = bisect.bisect_left(cts, start - WEEK)
        hi = bisect.bisect_right(cts, start)
        q = cl[lo:hi]
        if q:
            best = max(q, key=lambda c: (c[0], [-b for b in c[1].encode()] + [1]))
            best = sorted(q, key=lambda c: (-c[0], c[1].encode()))[0]
            camp = best[2]
        else:
            camp = "organic"
        x = cd[(camp, day)]
        x[0] += 1; x[1] += int(purch); x[2] += rev
        for c in {c[2] for c in q}:
            cd[(c, day)][3] += 1

os.makedirs(OUT, exist_ok=True)
con = duckdb.connect()
import datetime
ep = datetime.date(1970, 1, 1)
urows = []
for (per, day), (s, n, rev, cats) in ud.items():
    top = sorted(cats.items(), key=lambda kv: (-kv[1], kv[0].encode()))[0][0] if cats else None
    urows.append((per, ep + datetime.timedelta(days=day), s, n, rev, top))
crows = [(c, "organic" if c == "organic" else campaigns[c], ep + datetime.timedelta(days=day), *v) for (c, day), v in cd.items()]
import pyarrow as pa
US = pa.schema([("user_id", pa.string()), ("day", pa.date32()), ("sessions", pa.int64()), ("events", pa.int64()),
                ("revenue_cents", pa.int64()), ("top_category", pa.string())])
CS = pa.schema([("campaign_id", pa.string()), ("channel", pa.string()), ("day", pa.date32()), ("sessions", pa.int64()),
                ("purchasing_sessions", pa.int64()), ("revenue_cents", pa.int64()), ("assisted_sessions", pa.int64())])
u = pa.Table.from_pylist([dict(zip(US.names, r)) for r in urows], schema=US)
c = pa.Table.from_pylist([dict(zip(CS.names, r)) for r in crows], schema=CS)
for t, nm in ((u, "user_daily"), (c, "campaign_daily")):
    os.makedirs(os.path.join(OUT, nm), exist_ok=True)
    con.register("t", t)
    con.execute(f"COPY t TO '{os.path.join(OUT, nm, 'part-0.parquet')}' (FORMAT parquet)")
    con.unregister("t")
