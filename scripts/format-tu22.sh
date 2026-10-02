#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/t_u22_cross_index_unique.go \
  hat/hatDataStructure/t_u22_cross_index_unique_test.go \
  hat/hatDataStructure/t_u22_cross_index_unique_benchmark_test.go \
  hat/hatDataStructure/t_u22_cross_index_unique_comparison_benchmark_test.go \
  hat/hatDataStructure/t_u22_cross_index_unique_baseline_benchmark_test.go
