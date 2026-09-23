#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat -- \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  T230_ON_REPLACE_CHANGEFEED.md \
  hat/hatDataStructure/space.go \
  hat/hatDataStructure/t230_space_on_replace_baseline_test.go \
  hat/hatDataStructure/t230_space_on_replace_test.go \
  scripts/benchmark-t230-before.sh \
  scripts/benchmark-t230.sh \
  scripts/commit-t230.sh \
  scripts/format-t230.sh \
  scripts/push-t230.sh \
  scripts/race-t230.sh \
  scripts/review-t230.sh \
  scripts/stage-t230.sh \
  scripts/test-t230-package.sh \
  scripts/test-t230.sh \
  scripts/verify-t230.sh \
  scripts/vet-t230.sh
