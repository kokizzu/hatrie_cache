#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/range_tuple_cache.go \
  hat/hatDataStructure/tt013_range_tuple_cache_test.go \
  hat/hatDataStructure/tt013_range_tuple_cache_baseline_benchmark_test.go \
  hat/hatDataStructure/tt013_range_tuple_cache_benchmark_test.go
