#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  TR026_BITMAP_INDEX.md \
  hat/hatDataStructure/tr026_bitmap_index.go \
  hat/hatDataStructure/tr026_bitmap_index_test.go \
  hat/hatDataStructure/tr026_bitmap_index_benchmark_test.go \
  hat/hatDataStructure/tr026_bitmap_index_feature_benchmark_test.go \
  scripts/benchmark-tr026-bitmap-index.sh \
  scripts/commit-tr026-bitmap-index.sh \
  scripts/format-tr026-bitmap-index.sh \
  scripts/push-tr026-bitmap-index.sh \
  scripts/race-tr026-bitmap-index.sh \
  scripts/review-tr026-bitmap-index.sh \
  scripts/test-tr026-bitmap-index.sh \
  scripts/vet-tr026-bitmap-index.sh
git diff --cached --check
git commit -m "Add typed bitmap index"
