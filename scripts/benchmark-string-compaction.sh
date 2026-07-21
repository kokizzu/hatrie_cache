#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
compaction_benchmark=${STRING_COMPACTION_BENCH:-^BenchmarkStringCompaction100k$}
gc_benchmark=${STRING_COMPACTION_GC_BENCH:-^BenchmarkStringCompactionPostGC100k$}
keys=${STRING_STORAGE_BENCH_KEYS:-100000}
benchtime=${BENCHTIME:-1x}
gc_benchtime=${STRING_COMPACTION_GC_BENCHTIME:-20x}
count=${COUNT:-7}
output="$artifact_dir/string-compaction.txt"

mkdir -p "$artifact_dir"
{
	echo "# String compaction"
	HATRIE_STRING_STORAGE_KEYS="$keys" go test . \
		-run '^$' \
		-bench "$compaction_benchmark" \
		-benchmem \
		-benchtime "$benchtime" \
		-count "$count"
	echo
	echo "# Post-compaction forced GC"
	HATRIE_STRING_STORAGE_KEYS="$keys" go test . \
		-run '^$' \
		-bench "$gc_benchmark" \
		-benchmem \
		-benchtime "$gc_benchtime" \
		-count "$count"
} | tee "$output"
