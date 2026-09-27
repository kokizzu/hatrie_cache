#!/usr/bin/env bash
set -euo pipefail
cache_dir="/tmp/hatrie-cache-bench-m090d"
tmp_dir="/tmp/hatrie-tmp-bench-m090d"
cleanup() {
	rm -rf "$cache_dir" "$tmp_dir"
}
trap cleanup EXIT
mkdir -p "$cache_dir" "$tmp_dir"
GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" go test ./hat/hatSql -run '^$' -bench 'BenchmarkM090dFilteredProjectedSource' -benchmem -count=5
