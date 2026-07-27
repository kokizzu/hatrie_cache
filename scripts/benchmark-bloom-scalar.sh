#!/usr/bin/env sh
set -eu

benchmark=${BLOOM_SCALAR_BENCH:-^BenchmarkBloomFilter(ScalarAddChecked|AddCheckedProduction|VariadicBatchControl)}
benchtime=${BENCHTIME:-1s}
count=${COUNT:-1}

go test . \
	-run '^$' \
	-bench="$benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1
