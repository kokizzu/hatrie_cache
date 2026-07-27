#!/usr/bin/env sh
set -eu

benchmark=${CMS_SCALAR_BENCH:-^BenchmarkCountMinSketch(ScalarAddChecked|AddCheckedProduction|VariadicBatchControl)}
benchtime=${BENCHTIME:-1s}
count=${COUNT:-1}

go test . \
	-run '^$' \
	-bench="$benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1
