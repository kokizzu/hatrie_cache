#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatDataStructure/t218_multi_part_tree_index_benchmark_test.go \
	hat/hatDataStructure/t218_multi_part_tree_index_feature_benchmark_test.go \
	hat/hatDataStructure/t218_multi_part_tree_index_test.go \
	hat/hatDataStructure/multi_part_tree_index.go \
	hat/hatDataStructure/ordered_index.go
