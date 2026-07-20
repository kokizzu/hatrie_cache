#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${ATOMIC_RESTORE_BENCH:-^BenchmarkSinglePassAtomicRestore10k$}
keys=${BACKUP_BENCH_KEYS:-10000}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-7}
output="$artifact_dir/single-pass-atomic-restore.txt"

mkdir -p "$artifact_dir"
echo "Atomic restore benchmark: bench=$benchmark keys=$keys benchtime=$benchtime count=$count"
echo
HATRIE_BACKUP_BENCH_KEYS="$keys" go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"

echo
echo "Raw result: $output"
