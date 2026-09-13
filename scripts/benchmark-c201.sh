#!/usr/bin/env bash
set -euo pipefail

output="${C201_BENCHMARK_OUTPUT:-/tmp/hatrie-cache-c201-benchmark.txt}"
mkdir -p "$(dirname "$output")"
go test ./hat/hatPipeline -run '^$' -bench '^(BenchmarkAsyncBatcherSubmit|BenchmarkC201)' -benchmem -count=5 -benchtime="${C201_BENCHTIME:-1s}" -cpu=1 >"$output"
cat "$output"
