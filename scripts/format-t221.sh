#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/tr026_bitmap_index.go \
  hat/hatDataStructure/tr026_bitmap_index_benchmark_test.go \
  hat/hatDataStructure/tr026_bitmap_index_feature_benchmark_test.go \
  hat/hatDataStructure/tr026_bitmap_index_test.go
