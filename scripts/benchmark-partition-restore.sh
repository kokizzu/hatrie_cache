#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${PARTITION_RESTORE_BENCH:-^BenchmarkLocalPartitionRestore100k$}
keys=${PARTITION_RESTORE_BENCH_KEYS:-100000}
partitions=${PARTITION_RESTORE_COUNT:-16}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-7}
output="$artifact_dir/partition-restore.txt"

mkdir -p "$artifact_dir"
echo "Partition restore benchmark: bench=$benchmark keys=$keys partitions=$partitions benchtime=$benchtime count=$count"
echo
HATRIE_PARTITION_RESTORE_KEYS="$keys" \
HATRIE_PARTITION_RESTORE_COUNT="$partitions" \
go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"

echo
echo "Raw result: $output"
