#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${SCALAR_NATIVE_BATCH_BENCH:-^BenchmarkScalarNativeBatch$}
benchtime=${BENCHTIME:-2s}
count=${COUNT:-7}
output="$artifact_dir/scalar-native-batch.txt"

mkdir -p "$artifact_dir"
go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"
