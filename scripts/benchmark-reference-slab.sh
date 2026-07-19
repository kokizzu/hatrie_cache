#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${REFERENCE_SLAB_BENCH:-^BenchmarkLevelDBReferenceRetainedMemory100k$}
benchtime=${BENCHTIME:-3x}
count=${COUNT:-5}
output="$artifact_dir/lazy-reference-slab.txt"

mkdir -p "$artifact_dir"
go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"
