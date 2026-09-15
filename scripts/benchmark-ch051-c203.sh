#!/usr/bin/env bash
set -euo pipefail

pattern="${BENCHMARK_PATTERN:-Benchmark(LowCardinalityString|PlainString)}"
count="${BENCHMARK_COUNT:-5}"
go test ./hat/hatDataStructure -run '^$' -bench "$pattern" -benchmem -count="$count"
