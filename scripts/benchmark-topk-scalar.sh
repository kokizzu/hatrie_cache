#!/usr/bin/env sh
set -eu

benchmark=${TOP_K_SCALAR_BENCH:-^BenchmarkTopKGenericScalarDispatch}
benchtime=${BENCHTIME:-1s}
count=${COUNT:-1}

go test . \
	-run '^$' \
	-bench="$benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1
