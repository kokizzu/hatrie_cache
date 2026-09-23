#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  T226_INDEX_HINTS.md \
  TU26_INDEX_STRATEGY_INSPECTION.md \
  scripts/benchmark-t226.sh \
  scripts/commit-t226.sh \
  scripts/format-t226.sh \
  scripts/push-t226.sh \
  scripts/race-t226.sh \
  scripts/review-t226.sh \
  scripts/stage-t226.sh \
  scripts/test-t226-package.sh \
  scripts/test-t226.sh \
  scripts/verify-t226.sh \
  scripts/vet-t226.sh
