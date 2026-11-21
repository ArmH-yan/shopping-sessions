#!/bin/sh
# The output must not depend on thread count or scheduling (a ROWS-vs-RANGE kind of bug).
# Usage: scripts/determinism.sh DATA_DIR [BINARY]
set -eu
DATA=$1; BIN=${2:-fast/target/release/shopping}
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
for n in 1 3 8 16; do DATA_DIR=$DATA OUTPUT_DIR=$T/n$n NCPU=$n "$BIN"; done
for n in 3 8 16; do python3 scripts/compare.py "$T/n1" "$T/n$n" | tail -1 | grep -q MATCH || { echo "FAIL: NCPU=$n differs from NCPU=1"; exit 1; }; done
echo "ok   identical output with 1, 3, 8 and 16 threads"
