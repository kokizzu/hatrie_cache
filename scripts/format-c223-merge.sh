#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/quantile.go \
  hat/hatDataStructure/quantile_merge.go \
  hat/hatDataStructure/c223_merge_test.go \
  hat/hatDataStructure/c223_merge_benchmark_test.go
