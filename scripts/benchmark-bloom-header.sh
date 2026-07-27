#!/usr/bin/env sh
set -eu

layout_benchmark=${BLOOM_HEADER_LAYOUT_BENCH:-^BenchmarkBloomFilterHeaderLayout100k$}
operation_benchmark=${BLOOM_HEADER_OPERATION_BENCH:-^BenchmarkBloomFilter(AddKey|ContainsKey)$}
layout_benchtime=${BLOOM_HEADER_LAYOUT_BENCHTIME:-1x}
benchtime=${BENCHTIME:-500ms}
count=${COUNT:-1}

go test . \
	-run '^$' \
	-bench="$layout_benchmark" \
	-benchmem \
	-benchtime="$layout_benchtime" \
	-count="$count" \
	-cpu=1

go test . \
	-run '^$' \
	-bench="$operation_benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1
