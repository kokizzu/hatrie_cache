#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat -- \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  T229_BEFORE_REPLACE.md \
  hat/hatDataStructure/space.go \
  hat/hatDataStructure/t229_space_before_replace_baseline_test.go \
  hat/hatDataStructure/t229_space_before_replace_test.go \
  scripts/benchmark-t229-before.sh \
  scripts/benchmark-t229.sh \
  scripts/commit-t229.sh \
  scripts/format-t229.sh \
  scripts/push-t229.sh \
  scripts/race-t229.sh \
  scripts/review-t229.sh \
  scripts/stage-t229.sh \
  scripts/test-t229-package.sh \
  scripts/test-t229.sh \
  scripts/verify-t229.sh \
  scripts/vet-t229.sh
