#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${EXISTING_RECOVERY_BENCH:-^BenchmarkExistingReplicaRecovery10k$}
keys=${BACKUP_BENCH_KEYS:-10000}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-7}
output="$artifact_dir/existing-replica-recovery.txt"

mkdir -p "$artifact_dir"
echo "Existing replica recovery benchmark: bench=$benchmark keys=$keys changed=1% benchtime=$benchtime count=$count"
echo
HATRIE_RECOVERY_BENCH_KEYS="$keys" \
go test ./cmd/hatrie-cache \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"

echo
echo "Raw result: $output"
