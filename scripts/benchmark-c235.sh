#!/usr/bin/env bash
set -euo pipefail

cache_dir="${TMPDIR:-/tmp}/hatrie-cache-c235-gocache"
rm -rf "$cache_dir"
mkdir -p "$cache_dir"
trap 'rm -rf "$cache_dir"' EXIT
printf '%s\n' 'running C235 baseline benchmark'
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkC235(BaselineQueryProfilerRecord|SQLTaskProfilerRecord)$' -benchmem -count=5 -v
printf '%s\n' 'finished C235 baseline benchmark'
