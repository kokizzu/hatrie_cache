#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  T231_AFTER_REPLACE_AUDIT.md \
  hat/hatDataStructure/space.go \
  hat/hatDataStructure/t231_space_after_replace_baseline_test.go \
  hat/hatDataStructure/t231_space_after_replace_test.go \
  scripts/benchmark-t231-before.sh \
  scripts/benchmark-t231.sh \
  scripts/commit-t231.sh \
  scripts/format-t231.sh \
  scripts/push-t231.sh \
  scripts/race-t231.sh \
  scripts/review-t231.sh \
  scripts/stage-t231.sh \
  scripts/test-t231-package.sh \
  scripts/test-t231.sh \
  scripts/verify-t231.sh \
  scripts/vet-t231.sh
git diff --cached --check
