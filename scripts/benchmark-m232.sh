#!/bin/sh
set -eu

tmp=$(mktemp -d /tmp/hatrie-cache-m232-benchmark.XXXXXX)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/gocache" "$tmp/gotmp"
BENCHTIME="${BENCHTIME:-200ms}"
COUNT="${COUNT:-5}"
OUTPUT="${OUTPUT:-M232_BENCHMARK_RAW.txt}"
GOCACHE="$tmp/gocache" GOTMPDIR="$tmp/gotmp" go test -run '^$' -bench '^BenchmarkM232' -benchmem -benchtime="$BENCHTIME" -count="$COUNT" ./hat/hatSql | tee "$OUTPUT"
