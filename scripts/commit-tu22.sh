#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU22_CROSS_INDEX_UNIQUENESS.md \
  hat/hatDataStructure/tu22_unique_constraint_baseline_benchmark_test.go \
  hat/hatDataStructure/tu22_unique_constraint_benchmark_test.go \
  hat/hatDataStructure/tu22_unique_constraint_test.go \
  hat/hatDataStructure/unique_constraints.go \
  scripts/benchmark-tu22.sh \
  scripts/commit-tu22.sh \
  scripts/format-tu22.sh \
  scripts/push-tu22.sh \
  scripts/race-tu22.sh \
  scripts/test-tu22.sh \
  scripts/verify-tu22.sh \
  scripts/vet-tu22.sh
git commit -m "feat: add atomic cross-index uniqueness registry"
