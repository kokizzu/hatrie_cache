#!/usr/bin/env bash
set -euo pipefail

go test \
  hat/hatDataStructure/sparse_bitset.go \
  hat/hatDataStructure/sparse_bitset_values_benchmark_test.go \
  -run '^$' \
  -bench '^BenchmarkSparseBitsetValues$' \
  -benchmem \
  -count=10
