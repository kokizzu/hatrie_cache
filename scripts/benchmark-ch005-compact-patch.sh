#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-cache-ch005-compact-bench-XXXXXX)
output=$(mktemp /tmp/hatrie-cache-ch005-compact-output-XXXXXX)
trap 'rm -rf "$cache_dir" "$output"' EXIT
set +e
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH005PatchSnapshot(Marshal|Restore)(Cold)?$' -benchmem -count=5 -benchtime=100ms -v >"$output" 2>&1
status=$?
set -e
sed -n '1,200p' "$output"
exit "$status"
