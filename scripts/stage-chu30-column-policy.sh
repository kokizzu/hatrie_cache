#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  CHU30_COLUMN_AWARE_REMOTE_READ.md \
  PRODUCT_IDEA_GAPS.md \
  hat/hatStorage/remote_part.go \
  hat/hatStorage/remote_part_cache.go \
  hat/hatStorage/remote_part_cache_test.go \
  hat/hatStorage/remote_part_cache_baseline_test.go \
  scripts/format-chu30-column-policy.sh \
  scripts/test-chu30-column-policy.sh \
  scripts/test-chu30-storage.sh \
  scripts/benchmark-chu30-baseline.sh \
  scripts/benchmark-chu30-column.sh \
  scripts/race-chu30-column-policy.sh \
  scripts/vet-chu30-column-policy.sh \
  scripts/review-chu30-column-policy.sh \
  scripts/stage-chu30-column-policy.sh \
  scripts/commit-chu30-column-policy.sh \
  scripts/push-chu30-column-policy.sh \
  Makefile
