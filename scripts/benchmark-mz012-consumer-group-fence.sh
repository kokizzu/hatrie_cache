#!/usr/bin/env bash
set -euo pipefail

output=$(mktemp /tmp/hatrie-mz012-benchmark.XXXXXX)
trap 'rm -f "$output"' EXIT

go test ./hat/hatPipeline -run '^$' -bench 'BenchmarkMZ012' -benchmem -count=5 > "$output" 2>&1
sed -n '1,180p' "$output"
