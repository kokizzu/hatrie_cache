#!/usr/bin/env sh
set -eu

benchmark=${PRIORITY_QUEUE_SCALAR_BENCH:-^BenchmarkPriorityQueueScalarPush}
benchtime=${BENCHTIME:-1s}
count=${COUNT:-1}

go test ./hat/hatCache \
	-run '^$' \
	-bench="$benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1
