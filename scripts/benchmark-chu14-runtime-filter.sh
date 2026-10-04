#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
cache_dir=${GOCACHE:-$root/build/go-cache/chu14-runtime-filter}
mkdir -p "$root/$artifact_dir" "$cache_dir"
output="$root/$artifact_dir/chu14-runtime-filter.txt"
GOCACHE="$cache_dir" go -C "$root" test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRuntimeJoinFilter' -benchmem -benchtime="${BENCHTIME:-200ms}" -count="${BENCHCOUNT:-5}" > "$output"
cat "$output"
