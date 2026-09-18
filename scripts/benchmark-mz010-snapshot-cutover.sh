#!/usr/bin/env bash
set -euo pipefail

output="$(mktemp /tmp/hatrie-mz010-benchmark.XXXXXX)"
cleanup() {
	rm -f -- "$output"
}
trap cleanup EXIT

go test ./hat/hatPipeline -run '^$' -bench 'BenchmarkMZ010' -benchmem -count=5 -v > "$output" 2>&1
printf 'benchmark output bytes: '
wc -c < "$output"
sed -n '1,160p' "$output"
