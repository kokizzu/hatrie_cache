#!/usr/bin/env bash
set -euo pipefail

go test \
  hat/hatDataStructure/top_k.go \
  hat/hatDataStructure/top_k_entries_into_benchmark_test.go \
  -run '^$' \
  -bench '^BenchmarkTopKEntries' \
  -benchmem \
  -count=10
