#!/bin/sh
set -eu

tmp=$(mktemp -d /tmp/hatrie-cache-m228-benchmark.XXXXXX)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/gocache" "$tmp/gotmp"
GOCACHE="$tmp/gocache" GOTMPDIR="$tmp/gotmp" go test -run '^$' -bench '^BenchmarkM228' -benchmem -count=5 ./hat/hatCache
