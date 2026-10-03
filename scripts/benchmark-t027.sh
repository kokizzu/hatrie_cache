#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-t027-bench-cache.XXXXXX")"
temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-t027-bench-tmp.XXXXXX")"
trap 'rm -rf "$cache_dir" "$temp_dir"' EXIT

cd "$repo_dir"
GOCACHE="$cache_dir" GOTMPDIR="$temp_dir" go test ./hat/hatPeer ./hat/hatCache \
	-run '^$' \
	-bench '^BenchmarkT027' \
	-benchmem \
	-count=5 \
	> T027_BENCHMARK_RAW.txt
sed -i 's/[[:space:]]*$//' T027_BENCHMARK_RAW.txt
cat T027_BENCHMARK_RAW.txt
