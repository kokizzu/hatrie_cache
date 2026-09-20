#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/sparse_primary_index.go \
  hat/hatDataStructure/tu54_sparse_index_test.go \
  hat/hatDataStructure/tu54_sparse_index_benchmark_test.go
