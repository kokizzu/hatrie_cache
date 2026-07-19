#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${NATIVE_COMMAND_BATCH_BENCH:-^BenchmarkNativeCScalarBatch4096$}
benchtime=${BENCHTIME:-20x}
count=${COUNT:-5}
output="$artifact_dir/native-c-command-batch.txt"

mkdir -p "$artifact_dir"
go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"
