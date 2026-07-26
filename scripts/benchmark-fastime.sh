#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${CLOCK_BENCH:-^Benchmark(ClockSource|TrieClockSource)$}
benchtime=${BENCHTIME:-1s}
count=${COUNT:-7}
output="$artifact_dir/fastime.txt"

mkdir -p "$artifact_dir"
go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"
