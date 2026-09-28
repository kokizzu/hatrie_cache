#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure/top_k.go ./hat/hatDataStructure/top_k_benchmark_test.go -run '^$' -bench '^BenchmarkTopK' -benchmem -count=5
