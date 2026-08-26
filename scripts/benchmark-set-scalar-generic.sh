#!/usr/bin/env sh
set -eu

benchmark=${SET_SCALAR_GENERIC_BENCH:-^BenchmarkSetScalarGeneric(Add|ProductionControls)}
benchtime=${BENCHTIME:-1s}
count=${COUNT:-1}

go test ./hat/hatCache \
	-run '^$' \
	-bench="$benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1
