#!/usr/bin/env bash
set -euo pipefail

count="${BENCHMARK_COUNT:-5}"
go test ./hat/hatFiber -run '^$' -bench '^BenchmarkT235WorkerPool$' -benchmem -count="$count"
