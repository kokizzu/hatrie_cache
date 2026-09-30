#!/bin/sh
set -eu

tmp=$(mktemp -d /tmp/hatrie-cache-chg01-benchmark.XXXXXX)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/gocache" "$tmp/gotmp"
cd /tmp/hatrie-cache-chg01
GOCACHE="$tmp/gocache" GOTMPDIR="$tmp/gotmp" go test -run '^$' -bench '^BenchmarkCHG01' -benchtime=200ms -count=5 ./hat/hatSql > CHG01_BENCHMARK_RAW.txt
cat CHG01_BENCHMARK_RAW.txt
