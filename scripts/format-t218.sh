#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/multi_part_tree_index.go \
  hat/hatDataStructure/t218_multi_part_tree_index_test.go \
  hat/hatDataStructure/t218_multi_part_tree_index_benchmark_test.go
