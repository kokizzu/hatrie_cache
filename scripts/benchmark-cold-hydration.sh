#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${COLD_HYDRATION_BENCH:-^BenchmarkColdReferenceParallelHydration32$}
benchtime=${BENCHTIME:-5x}
count=${COUNT:-5}
output="$artifact_dir/cold-reference-hydration.txt"

mkdir -p "$artifact_dir"
go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"
