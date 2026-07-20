#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${CHECKPOINT_BOOTSTRAP_BENCH:-^BenchmarkCheckpointReplicaBootstrap10k$}
keys=${BACKUP_BENCH_KEYS:-10000}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-7}
output="$artifact_dir/checkpoint-replica-bootstrap.txt"

mkdir -p "$artifact_dir"
echo "Checkpoint bootstrap benchmark: bench=$benchmark keys=$keys benchtime=$benchtime count=$count"
echo
HATRIE_BACKUP_BENCH_KEYS="$keys" go test ./cmd/hatrie-cache \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"

echo
echo "Raw result: $output"
