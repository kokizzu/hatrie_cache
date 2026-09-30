#!/bin/sh
set -eu

tmp=$(mktemp -d /tmp/hatrie-cache-m231-benchmark.XXXXXX)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/gocache" "$tmp/gotmp"
BENCHTIME="${BENCHTIME:-200ms}"
COUNT="${COUNT:-5}"
OUTPUT="${OUTPUT:-M231_BENCHMARK_RAW.txt}"
GOCACHE="$tmp/gocache" GOTMPDIR="$tmp/gotmp" go test -run '^$' -bench '^BenchmarkM231' -benchmem -benchtime="$BENCHTIME" -count="$COUNT" ./hat/hatCache | tee "$OUTPUT"
