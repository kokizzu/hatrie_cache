#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${PARTITION_CURSOR_BENCH:-^BenchmarkPartitionReplicationPageTraversal100k$}
keys=${PARTITION_CURSOR_BENCH_KEYS:-100000}
page_size=${PARTITION_CURSOR_BENCH_PAGE_SIZE:-1000}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-7}
output="$artifact_dir/partition-replication-cursor.txt"

mkdir -p "$artifact_dir"
echo "Partition replication cursor benchmark: bench=$benchmark keys=$keys page_size=$page_size benchtime=$benchtime count=$count"
echo
HATRIE_PARTITION_CURSOR_BENCH_KEYS="$keys" \
HATRIE_PARTITION_CURSOR_BENCH_PAGE_SIZE="$page_size" \
go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"

echo
echo "Raw result: $output"
