#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/multikey_index.go \
  hat/hatDataStructure/t_u23_multikey_index_test.go \
  hat/hatDataStructure/t_u23_multikey_baseline_benchmark_test.go \
  hat/hatDataStructure/t_u23_multikey_benchmark_test.go
