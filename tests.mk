# Test and benchmark targets (no Docker needed). Run: make -f tests.mk test
# Needs cargo, python3 with duckdb + polars, and the generator's `generate` on PATH.
FIXTURES ?= fixtures
BIN   ?= fast/target/release/shopping
SEEDS ?= 2 3 15
EVENTS ?= 1000000
DEV   ?= dev
ANSWER ?= dev-answer

.PHONY: rust test test-unit test-fixtures test-cross test-determinism test-fallback test-dev bench

rust:
	cd fast && cargo build --release

## Everything below: make test (needs cargo, python3 with duckdb + polars, and `generate` on PATH)
test: test-unit test-fixtures test-fallback test-determinism test-cross

## Rust unit tests: CSV dialect, timestamps, prices, ID codes and ordering
test-unit:
	cd fast && cargo test --release

## The 36 hand-written fixtures, against the Rust binary, the DuckDB pipeline and the reference
test-fixtures: rust
	python3 scripts/run_fixtures.py $(FIXTURES) -- $(BIN)
	python3 scripts/run_fixtures.py $(FIXTURES) -- python3 pipeline/pipeline.py
	python3 scripts/run_fixtures.py $(FIXTURES) -- python3 scripts/reference.py

## run.sh falls back to DuckDB when the Rust binary cannot represent an ID exactly
test-fallback: rust
	sh scripts/test_fallback.sh

## Same output with 1, 3, 8 and 16 threads, on a generated dataset
test-determinism: rust
	rm -rf /tmp/ss-det && generate --seed 5 --events 500000 --clicks 125000 --output /tmp/ss-det
	sh scripts/determinism.sh /tmp/ss-det $(BIN)

## Generated seeds: Rust vs DuckDB vs reference, plus the rules' self-checks
test-cross: rust
	python3 scripts/crosscheck.py --seeds $(SEEDS) --events $(EVENTS) --duckdb --reference

## The public dev set against the published answer (dev-answer/ unzipped here)
test-dev: rust
	rm -rf out && DATA_DIR=$(DEV) OUTPUT_DIR=out $(BIN) && python3 scripts/compare.py out $(ANSWER)

## Best-of-3 wall time on a dataset: make bench DATA=dev
DATA ?= $(DEV)
bench: rust
	sh scripts/bench.sh $(DATA) 3
