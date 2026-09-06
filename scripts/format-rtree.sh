#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/rtree.go hat/hatDataStructure/rtree_test.go hat/hatDataStructure/rtree_public_test.go hat/hatDataStructure/rtree_benchmark_test.go
