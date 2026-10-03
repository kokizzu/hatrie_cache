#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cache_dir="$(mktemp -d /tmp/hatrie-cache-benchmark-t025.XXXXXX)"
temp_dir="$(mktemp -d /tmp/hatrie-cache-benchmark-t025-go.XXXXXX)"
raw_file="$repo_dir/T025_BENCHMARK_RAW.txt"
trap 'rm -rf "$cache_dir" "$temp_dir"' EXIT

cd "$repo_dir"
GOCACHE="$cache_dir" GOTMPDIR="$temp_dir" go test ./hat/hatSchema -run '^$' -bench '^BenchmarkT025' -benchmem -count=1 > "$raw_file"
sed -i 's/[[:space:]]*$//' "$raw_file"
cat "$raw_file"
