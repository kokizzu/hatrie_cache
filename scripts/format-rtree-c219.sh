#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/rtree.go hat/hatDataStructure/rtree_search_fastpath_test.go hat/hatDataStructure/rtree_search_fastpath_benchmark_test.go
