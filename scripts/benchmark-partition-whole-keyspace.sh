#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark=${PARTITION_WHOLE_KEYSPACE_BENCH:-^BenchmarkLocalPartitionWholeKeyspace100k$}
keys=${PARTITION_SCAN_BENCH_KEYS:-100000}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-7}
output="$artifact_dir/partition-whole-keyspace.txt"

mkdir -p "$artifact_dir"
echo "Partition whole-keyspace benchmark: bench=$benchmark keys=$keys benchtime=$benchtime count=$count"
echo
HATRIE_PARTITION_SCAN_BENCH_KEYS="$keys" go test . \
	-run '^$' \
	-bench "$benchmark" \
	-benchmem \
	-benchtime "$benchtime" \
	-count "$count" | tee "$output"

echo
echo "Raw result: $output"
