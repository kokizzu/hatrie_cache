#!/usr/bin/env bash
set -euo pipefail

cache_dir=/tmp/hatrie-cache-mz028-adaptive-compaction-gocache
cleanup() {
  rm -rf "$cache_dir"
}
trap cleanup EXIT HUP INT TERM

rm -rf "$cache_dir"
mkdir -p "$cache_dir"
GOCACHE="$cache_dir" go test ./hat/hatDataStructure \
  -run '^$' \
  -bench '^BenchmarkMZ028SpillableCompact' \
  -benchmem \
  -benchtime=50x \
  -count=5
