#!/usr/bin/env bash
set -euo pipefail

output="$(mktemp /tmp/hatrie-mz011-benchmark.XXXXXX)"
cleanup() {
	rm -f -- "$output"
}
trap cleanup EXIT

go test ./hat/hatPipeline -run '^$' -bench 'BenchmarkMZ011' -benchmem -count=5 > "$output" 2>&1
sed -n '1,180p' "$output"
