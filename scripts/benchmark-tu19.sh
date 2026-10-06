#!/usr/bin/env bash
set -euo pipefail
cache_dir=/tmp/hatrie-tu19-benchmark-cache-20261006
rm -rf "$cache_dir"
mkdir -p "$cache_dir"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test -run '^$' -bench '^BenchmarkTU19' -benchmem -count=5 -benchtime=250ms ./hat/hatDataStructure ./hat/hatCache
