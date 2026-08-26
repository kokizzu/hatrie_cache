#!/usr/bin/env sh
set -eu

path_benchmark=${RESERVOIR_BATCH_PATH_BENCH:-^BenchmarkReservoirSample(ExistingBatchCommandPath|BatchCommandPath)$}
alternating_benchmark=${RESERVOIR_BATCH_ALTERNATING_BENCH:-^BenchmarkReservoirSampleCommandBatchAlternating$}
benchtime=${BENCHTIME:-10000x}
alternating_benchtime=${RESERVOIR_BATCH_ALTERNATING_BENCHTIME:-100x}
count=${COUNT:-1}

go test ./hat/hatCache \
	-run '^$' \
	-bench="$path_benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1

go test ./hat/hatCache \
	-run '^$' \
	-bench="$alternating_benchmark" \
	-benchtime="$alternating_benchtime" \
	-count="$count" \
	-cpu=1
