#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${MERKLE_MAINTENANCE_BENCH:-^BenchmarkReplicationMerkle(ChurnSnapshotCycle|SnapshotAfterChurn)$}
write_benchtime=${MERKLE_WRITE_BENCHTIME:-100000x}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-7}
output="$artifact_dir/merkle-maintenance.txt"

mkdir -p "$artifact_dir"
go test . \
	-run '^$' \
	-bench '^BenchmarkReplicationMerkleWriteTracking$' \
	-benchmem \
	-benchtime "$write_benchtime" \
	-count "$count" | tee "$output"
go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee -a "$output"
