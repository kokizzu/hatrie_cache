#!/usr/bin/env sh
set -eu

benchmark=${COUNT_MIN_ROWS_BENCH:-^BenchmarkCountMinSketch(DirectRows|JSONStringRows)$}
benchtime=${BENCHTIME:-500ms}
count=${COUNT:-1}

go test ./hat/hatCache \
	-run '^$' \
	-bench="$benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1
