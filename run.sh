#!/bin/sh
# Entry point. Runs with no arguments; exit 0 means "done, check my output".
# The Rust binary handles every dataset the generator makes. If it exits non-zero for any
# reason (exit 3: an ID it cannot represent exactly), the DuckDB pipeline redoes the work.
set -u
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
if /app/shopping; then
  exit 0
fi
rm -rf "$OUTPUT_DIR/user_daily" "$OUTPUT_DIR/campaign_daily"
exec python3 /app/pipeline/pipeline.py
