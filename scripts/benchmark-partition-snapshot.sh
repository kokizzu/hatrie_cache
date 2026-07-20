#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${PARTITION_SNAPSHOT_BENCH:-^BenchmarkBigWins/Snapshot$}
keys=${PARTITION_SNAPSHOT_BENCH_KEYS:-100000}
partitions=${PARTITION_SNAPSHOT_COUNT:-16}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-7}
output="$artifact_dir/partition-snapshot-capture.txt"

mkdir -p "$artifact_dir"
echo "Partition snapshot benchmark: bench=$benchmark keys=$keys partitions=$partitions benchtime=$benchtime count=$count"
echo
HATRIE_BIG_WINS_KEYS="$keys" \
HATRIE_BIG_WINS_SNAPSHOT_PARTITIONS="$partitions" \
go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"

echo
echo "Raw result: $output"
