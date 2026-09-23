#!/usr/bin/env bash
set -euo pipefail

output=.t234-benchmark-output
trap 'rm -f "$output"' EXIT
go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkT234' -benchmem -count=5 >"$output"
rg '^(goos|goarch|pkg|cpu|Benchmark|PASS|ok)' "$output"
