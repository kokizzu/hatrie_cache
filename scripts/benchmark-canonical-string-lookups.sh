#!/usr/bin/env sh
set -eu

benchmark=${CANONICAL_STRING_LOOKUP_BENCH:-^BenchmarkPublicCanonicalStringLookups}
benchtime=${BENCHTIME:-1s}
count=${COUNT:-1}

go test . \
	-run '^$' \
	-bench="$benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1
