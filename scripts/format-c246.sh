#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/tt021_packed_rtree.go \
  hat/hatDataStructure/tt021_packed_rtree_baseline_test.go \
  hat/hatDataStructure/tt021_packed_rtree_benchmark_test.go \
  hat/hatDataStructure/tt021_packed_rtree_test.go
