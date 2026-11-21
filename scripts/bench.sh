#!/bin/sh
# Best-of-N wall time of the Rust binary: scripts/bench.sh DATA_DIR [N]
set -eu
DATA=$1; N=${2:-3}; BIN=${BIN:-fast/target/release/shopping}
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
cat "$DATA"/events/*.csv "$DATA"/clicks/*.csv > /dev/null   # warm the page cache
best=""
for i in $(seq "$N"); do
  rm -rf "$T/out"
  s=$(date +%s%N); DATA_DIR=$DATA OUTPUT_DIR=$T/out "$BIN"; e=$(date +%s%N)
  ms=$(( (e - s) / 1000000 )); echo "run $i: ${ms} ms"
  if [ -z "$best" ] || [ "$ms" -lt "$best" ]; then best=$ms; fi
done
echo "best: ${best} ms"
