#!/usr/bin/env sh
set -eu

benchmark=${QUANTILE_ADD_BENCH:-^BenchmarkQuantileSketchAddValidation$}
benchtime=${BENCHTIME:-500ms}
count=${COUNT:-1}

go test ./hat/hatCache \
	-run '^$' \
	-bench="$benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1
