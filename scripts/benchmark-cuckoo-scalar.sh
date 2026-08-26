#!/usr/bin/env sh
set -eu

benchmark=${CUCKOO_SCALAR_BENCH:-^BenchmarkCuckooFilter(ScalarAddChecked|AddCheckedProduction|VariadicBatchControl|ScalarDelete)}
benchtime=${BENCHTIME:-1s}
count=${COUNT:-1}

go test ./hat/hatCache \
	-run '^$' \
	-bench="$benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1
