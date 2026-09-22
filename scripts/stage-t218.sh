#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION_ROUND2.md \
  BENCHMARK.md \
  T218_MULTI_PART_TREE_INDEX.md \
  hat/hatDataStructure/multi_part_tree_index.go \
  hat/hatDataStructure/t218_multi_part_tree_index_test.go \
  hat/hatDataStructure/t218_multi_part_tree_index_benchmark_test.go \
  scripts/format-t218.sh \
  scripts/test-t218.sh \
  scripts/benchmark-t218.sh \
  scripts/race-t218.sh \
  scripts/vet-t218.sh \
  scripts/verify-docs-t218.sh \
  scripts/stage-t218.sh \
  scripts/commit-t218.sh \
  scripts/push-t218.sh
git diff --cached --check
git diff --cached --name-status
