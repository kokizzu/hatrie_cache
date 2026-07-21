#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${STRING_STORAGE_BENCH:-^BenchmarkStringStorageLayout100k$}
keys=${STRING_STORAGE_BENCH_KEYS:-100000}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-7}
output="$artifact_dir/string-storage.txt"

mkdir -p "$artifact_dir"
HATRIE_STRING_STORAGE_KEYS="$keys" go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"
