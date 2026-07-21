#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${STRUCTURED_STORAGE_CODEC_BENCH:-^BenchmarkStructuredStorageFallback\(Encode\|Decode\)$}
benchtime=${BENCHTIME:-1000x}
count=${COUNT:-7}
output="$artifact_dir/structured-storage-codec.txt"

mkdir -p "$artifact_dir"
go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"
