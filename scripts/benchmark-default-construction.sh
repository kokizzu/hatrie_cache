#!/usr/bin/env sh
set -eu

benchmark=${DEFAULT_CONSTRUCTION_BENCH:-^BenchmarkHatTrieDefaultConstruction$}
benchtime=${BENCHTIME:-10000x}
count=${COUNT:-1}

go test . \
	-run '^$' \
	-bench="$benchmark" \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count" \
	-cpu=1
