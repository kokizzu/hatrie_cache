#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-t026-bench-cache.XXXXXX")"
temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-t026-bench-tmp.XXXXXX")"
trap 'rm -rf "$cache_dir" "$temp_dir"' EXIT

cd "$repo_dir"
GOCACHE="$cache_dir" GOTMPDIR="$temp_dir" go test ./hat/hatSql \
	-run '^$' \
	-bench '^BenchmarkT026' \
	-benchmem \
	-count=5 \
	> T026_BENCHMARK_RAW.txt
sed -i 's/[[:space:]]*$//' T026_BENCHMARK_RAW.txt
cat T026_BENCHMARK_RAW.txt
