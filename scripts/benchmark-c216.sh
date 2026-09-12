#!/usr/bin/env bash
set -euo pipefail

output="${C216_BENCHMARK_OUTPUT:-/tmp/hatrie-cache-c216-benchmark.txt}"
mkdir -p "$(dirname "$output")"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkC216' -benchmem -count=5 -benchtime="${C216_BENCHTIME:-200ms}" -cpu=1 >"$output"
cat "$output"
