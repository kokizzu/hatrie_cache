#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  TU54_SPARSE_PRIMARY_INDEX.md \
  hat/hatDataStructure/sparse_primary_index.go \
  hat/hatDataStructure/tu54_sparse_index_benchmark_test.go \
  hat/hatDataStructure/tu54_sparse_index_test.go \
  scripts/benchmark-tu54-sparse-index.sh \
  scripts/commit-tu54-sparse-index.sh \
  scripts/format-tu54-sparse-index.sh \
  scripts/push-tu54-sparse-index.sh \
  scripts/race-tu54-sparse-index.sh \
  scripts/review-tu54-sparse-index.sh \
  scripts/stage-tu54-sparse-index.sh \
  scripts/test-tu54-sparse-index-package.sh \
  scripts/test-tu54-sparse-index.sh \
  scripts/vet-tu54-sparse-index.sh
