#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure/quantile.go ./hat/hatDataStructure/quantile_merge.go ./hat/hatDataStructure/c223_merge_benchmark_test.go -run '^$' -bench '^BenchmarkC223' -benchmem -count=5
