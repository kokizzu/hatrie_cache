#!/bin/sh
set -eu

tmp=$(mktemp -d /tmp/hatrie-cache-m236-benchmark.XXXXXX)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/gocache" "$tmp/gotmp"
cd /tmp/hatrie-cache-m236
GOCACHE="$tmp/gocache" GOTMPDIR="$tmp/gotmp" go test -run '^$' -bench '^BenchmarkM236' -benchtime=200ms -count=5 ./hat/hatSql > M236_BENCHMARK_RAW.txt
cat M236_BENCHMARK_RAW.txt
