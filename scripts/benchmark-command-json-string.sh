#!/usr/bin/env sh
set -eu

benchmark=${COMMAND_JSON_STRING_BENCH:-^BenchmarkCommandCanonicalJSONString}
benchtime=${BENCHTIME:-1s}
count=${COUNT:-1}

go test ./hat/hatCache \
	-run '^$' \
	-bench="$benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1
