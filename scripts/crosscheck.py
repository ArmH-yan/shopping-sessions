#!/usr/bin/env python3
"""
Generate datasets with the official generator and check that every engine agrees exactly.

    python3 scripts/crosscheck.py --seeds 2 3 15 --events 1000000 [--duckdb] [--reference]

Engines: the Rust binary (always), the DuckDB pipeline (--duckdb) and the independent Python
reference (--reference, slow: about 20 s per million events). Each dataset is also checked
against the rules' self-check invariants. Seed 15's window holds 2028-02-29.
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from run_fixtures import SCHEMAS, load_actual  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


def run(name, cmd, data, out, extra_env=None):
    env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(out), SCRATCH_DIR=str(out) + "_scratch", **(extra_env or {}))
    t = time.perf_counter()
    p = subprocess.run(cmd, env=env, capture_output=True, text=True)
    dt = time.perf_counter() - t
    if p.returncode != 0:
        sys.exit(f"{name} failed (exit {p.returncode}):\n{p.stderr[-2000:]}")
    return dt


def load(out):
    return {t: load_actual(Path(out) / t, s) for t, s in SCHEMAS.items()}


def same(a, b):
    for t, s in SCHEMAS.items():
        x, y = a[t], b[t]
        if x.height != y.height:
            return f"{t}: {x.height} vs {y.height} rows"
        diff = x.join(y, on=list(s), how="anti", nulls_equal=True)
        if diff.height:
            return f"{t}: {diff.height} rows differ, e.g.\n{diff.head(3)}"
    return None


def invariants(tables):
    u, c = tables["user_daily"], tables["campaign_daily"]
    problems = []
    if u["sessions"].sum() != c["sessions"].sum():
        problems.append("sessions differ between tables")
    if u["revenue_cents"].sum() != c["revenue_cents"].sum():
        problems.append("revenue differs between tables")
    credited = c.filter(pl.col("campaign_id") != "organic")["sessions"].sum()
    if c["assisted_sessions"].sum() < credited:
        problems.append("assisted < credited non-organic sessions")
    if c.filter(pl.col("campaign_id") == "organic")["assisted_sessions"].sum() != 0:
        problems.append("organic assists")
    for t, keys in (("user_daily", ["user_id", "day"]), ("campaign_daily", ["campaign_id", "day"])):
        if tables[t].height != tables[t].n_unique(subset=keys):
            problems.append(f"duplicate keys in {t}")
    return problems


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, nargs="+", default=[2, 3, 15])
    ap.add_argument("--events", type=int, default=1_000_000)
    ap.add_argument("--clicks", type=int, default=0, help="default: events / 4")
    ap.add_argument("--generator", default=shutil.which("generate") or "generate")
    ap.add_argument("--binary", default=str(ROOT / "fast/target/release/shopping"))
    ap.add_argument("--duckdb", action="store_true")
    ap.add_argument("--reference", action="store_true")
    args = ap.parse_args()
    clicks = args.clicks or args.events // 4
    failed = False
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        for seed in args.seeds:
            data = tmp / f"seed{seed}"
            subprocess.run([args.generator, "--seed", str(seed), "--events", str(args.events),
                            "--clicks", str(clicks), "--output", str(data)], check=True, capture_output=True)
            outs, times = {}, {}
            times["rust"] = run("rust", [args.binary], data, tmp / f"rust{seed}")
            outs["rust"] = load(tmp / f"rust{seed}")
            if args.duckdb:
                times["duckdb"] = run("duckdb", [sys.executable, str(ROOT / "pipeline/pipeline.py")], data, tmp / f"duck{seed}")
                outs["duckdb"] = load(tmp / f"duck{seed}")
            if args.reference:
                times["reference"] = run("reference", [sys.executable, str(ROOT / "scripts/reference.py"), str(data), str(tmp / f"ref{seed}")], data, tmp / f"ref{seed}")
                outs["reference"] = load(tmp / f"ref{seed}")
            msgs = invariants(outs["rust"])
            for name in outs:
                if name != "rust":
                    d = same(outs["rust"], outs[name])
                    if d:
                        msgs.append(f"rust vs {name}: {d}")
            timing = ", ".join(f"{k} {v:.1f}s" for k, v in times.items())
            print(f"{'FAIL' if msgs else 'ok  '} seed {seed}: {outs['rust']['user_daily'].height} user rows ({timing})")
            for m in msgs:
                print("     ", m)
            failed |= bool(msgs)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
