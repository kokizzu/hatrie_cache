#!/usr/bin/env bash
set -euo pipefail

git diff --check -- hat/hatDataStructure/rtree.go hat/hatDataStructure/rtree_search_fastpath_test.go hat/hatDataStructure/rtree_search_fastpath_benchmark_test.go INSPIRATION.md BENCHMARK.md
git diff --stat -- hat/hatDataStructure/rtree.go hat/hatDataStructure/rtree_search_fastpath_test.go hat/hatDataStructure/rtree_search_fastpath_benchmark_test.go INSPIRATION.md BENCHMARK.md
git diff -- hat/hatDataStructure/rtree.go hat/hatDataStructure/rtree_search_fastpath_test.go hat/hatDataStructure/rtree_search_fastpath_benchmark_test.go
git status --short
