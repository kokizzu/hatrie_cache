#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-cache-gocache-tt024-benchmark.XXXXXX)
tmp_dir=$(mktemp -d /tmp/hatrie-cache-gotmp-tt024-benchmark.XXXXXX)
output=$(mktemp /tmp/hatrie-cache-tt024-benchmark-output.XXXXXX)
trap 'rm -rf -- "$cache_dir" "$tmp_dir" "$output"' EXIT
if ! GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" go test ./hat/hatCache -run '^$' -bench '^BenchmarkTT024TextIntersection' -benchmem -count=5 >"$output" 2>&1; then
	cat "$output"
	exit 1
fi
cat "$output"
