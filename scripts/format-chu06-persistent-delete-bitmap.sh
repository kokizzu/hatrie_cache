#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/persistent_delete_bitmap.go \
  hat/hatDataStructure/persistent_delete_bitmap_test.go \
  hat/hatDataStructure/persistent_delete_bitmap_baseline_benchmark_test.go \
  hat/hatDataStructure/persistent_delete_bitmap_benchmark_test.go
