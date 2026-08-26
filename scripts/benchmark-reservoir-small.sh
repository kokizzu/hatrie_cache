#!/usr/bin/env sh
set -eu

benchmark=${RESERVOIR_SMALL_BENCH:-^BenchmarkReservoirSampleSmallGetCommand$}
benchtime=${BENCHTIME:-500000x}
count=${COUNT:-1}

go test ./hat/hatCache \
	-run '^$' \
	-bench="$benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1
