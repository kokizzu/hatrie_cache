#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${PEBBLE_GENERATION_BENCH:-^BenchmarkPebbleFullSaveArchitecture10k$}
benchtime=${BENCHTIME:-3x}
count=${COUNT:-5}
output="$artifact_dir/pebble-full-save-generation.txt"

mkdir -p "$artifact_dir"
go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"
