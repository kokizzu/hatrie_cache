#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${LIVE_REPLICATION_BENCH:-^BenchmarkReplicationLiveTransport10K/grpc-stream$}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-7}
output="$artifact_dir/live-replication.txt"

mkdir -p "$artifact_dir"
go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"
