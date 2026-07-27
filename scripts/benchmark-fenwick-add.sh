#!/usr/bin/env sh
set -eu

benchmark=${FENWICK_ADD_BENCH:-^BenchmarkFenwickTree(AddTraversal|FirstAdd)$}
benchtime=${BENCHTIME:-500ms}
count=${COUNT:-1}

go test . \
	-run '^$' \
	-bench="$benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1
