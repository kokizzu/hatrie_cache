#!/usr/bin/env bash
set -euo pipefail

output=$(mktemp /tmp/hatrie-tr003-benchmark.XXXXXX)
trap 'rm -f "$output"' EXIT

go test ./hat/hatReplication -run '^$' -bench 'BenchmarkTR003' -benchmem -count=5 > "$output" 2>&1
sed -n '1,180p' "$output"
