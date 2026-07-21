#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${STARTUP_PERSISTENCE_BENCH:-^BenchmarkStartupPersistence10k$}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-7}
output="$artifact_dir/startup-persistence.txt"

mkdir -p "$artifact_dir"
go test ./cmd/hatrie-cache \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"
