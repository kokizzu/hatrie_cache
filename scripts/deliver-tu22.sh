#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU22_CROSS_INDEX_UNIQUENESS.md \
  hat/hatDataStructure/t_u22_cross_index_unique.go \
  hat/hatDataStructure/t_u22_cross_index_unique_baseline_benchmark_test.go \
  hat/hatDataStructure/t_u22_cross_index_unique_benchmark_test.go \
  hat/hatDataStructure/t_u22_cross_index_unique_comparison_benchmark_test.go \
  hat/hatDataStructure/t_u22_cross_index_unique_test.go \
  scripts/benchmark-tu22-before.sh \
  scripts/benchmark-tu22-comparison.sh \
  scripts/benchmark-tu22.sh \
  scripts/deliver-tu22.sh \
  scripts/format-tu22.sh \
  scripts/race-tu22.sh \
  scripts/status-tu22.sh \
  scripts/test-tu22-package.sh \
  scripts/test-tu22.sh \
  scripts/verify-tu22.sh \
  scripts/vet-tu22.sh
git diff --cached --check
git diff --cached --stat
git commit -m "feat: add atomic cross-index uniqueness [skip ci]"
git push -u origin codex/tu22-cross-index-unique
