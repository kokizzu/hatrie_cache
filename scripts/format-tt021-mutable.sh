#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/tt021_packed_rtree.go \
  hat/hatDataStructure/vertical_ttl_delete_test.go \
  hat/hatDataStructure/spillable_arrangement_test.go \
  hat/hatDataStructure/tt021_mutable_packed_rtree.go \
  hat/hatDataStructure/tt021_mutable_packed_rtree_test.go \
  hat/hatDataStructure/tt021_mutable_packed_rtree_baseline_benchmark_test.go \
  hat/hatDataStructure/tt021_mutable_packed_rtree_benchmark_test.go
