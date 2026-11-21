#!/bin/sh
# run.sh must fall back to the DuckDB pipeline when the Rust binary cannot run exactly
# (an ID over 15 bytes) and still produce the right answer.
set -eu
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
cp -r fixtures/L1/input "$T/data"
sed -i 's/,u3,/,u3_with_a_very_long_id,/' "$T/data/events/part-00000.csv"
mkdir -p "$T/app"; cp fast/target/release/shopping "$T/app/shopping"; cp -r pipeline "$T/app/pipeline"
sed "s#/app/#$T/app/#g" run.sh > "$T/app/run.sh"
DATA_DIR="$T/data" OUTPUT_DIR="$T/out" SCRATCH_DIR="$T/scratch" sh "$T/app/run.sh" >/dev/null
python3 - "$T/out" <<'PY'
import sys, polars as pl
u = pl.read_parquet(f"{sys.argv[1]}/user_daily/*.parquet").sort("user_id")
assert u["user_id"].to_list() == ["u1", "u3_with_a_very_long_id"], u
assert u["events"].to_list() == [3, 1], u
print("ok   fallback to DuckDB gives the right answer")
PY
