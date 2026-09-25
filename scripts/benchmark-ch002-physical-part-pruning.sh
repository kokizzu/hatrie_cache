#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
cache_dir="/tmp/hatrie-cache-ch002-benchmark-gocache"
output_file="/tmp/hatrie-cache-ch002-benchmark.txt"
trap 'rm -rf "$cache_dir" "$output_file"' EXIT
set +e
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH002PhysicalPart' -benchtime=200ms -count=5 >"$output_file"
status=$?
set -e
sed -n '1,240p' "$output_file"
printf 'benchmark exit status: %d\n' "$status"
exit "$status"
