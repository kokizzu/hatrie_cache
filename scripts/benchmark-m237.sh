#!/bin/sh
set -eu

tmp=$(mktemp -d /tmp/hatrie-cache-m237-benchmark.XXXXXX)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/gocache" "$tmp/gotmp"
cd /tmp/hatrie-cache-m237
GOCACHE="$tmp/gocache" GOTMPDIR="$tmp/gotmp" go test -run '^$' -bench '^BenchmarkM237' -benchtime=200ms -count=5 ./hat/hatSql > M237_BENCHMARK_RAW.txt
cat M237_BENCHMARK_RAW.txt
