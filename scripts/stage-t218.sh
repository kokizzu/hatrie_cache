#!/usr/bin/env bash
set -euo pipefail

make verify-t218-scope

paths=(
  Makefile
  README.md
  BENCHMARK.md
  INSPIRATION_ROUND2.md
  T218_MULTI_PART_TREE_INDEX.md
  hat/hatDataStructure/ordered_index.go
  hat/hatDataStructure/multi_part_tree_index.go
  hat/hatDataStructure/t218_multi_part_tree_index_benchmark_test.go
  hat/hatDataStructure/t218_multi_part_tree_index_feature_benchmark_test.go
  hat/hatDataStructure/t218_multi_part_tree_index_test.go
  scripts/format-t218.sh
  scripts/test-t218.sh
  scripts/benchmark-t218-before.sh
  scripts/benchmark-t218.sh
  scripts/test-t218-package.sh
  scripts/race-t218.sh
  scripts/vet-t218.sh
  scripts/verify-t218-scope.sh
  scripts/stage-t218.sh
  scripts/commit-t218.sh
  scripts/push-t218.sh
)

git add -- "${paths[@]}"
git diff --cached --name-only
