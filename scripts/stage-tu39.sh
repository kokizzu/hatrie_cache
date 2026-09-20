#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  TU39_SPACE_CHANGEFEED.md \
  hat/hatCache/space_changefeed.go \
  hat/hatCache/tu39_space_changefeed_benchmark_test.go \
  hat/hatCache/tu39_space_changefeed_test.go \
  scripts/benchmark-tu39-baseline.sh \
  scripts/benchmark-tu39.sh \
  scripts/commit-tu39.sh \
  scripts/format-tu39.sh \
  scripts/push-tu39.sh \
  scripts/race-tu39.sh \
  scripts/review-tu39.sh \
  scripts/stage-tu39.sh \
  scripts/test-tu39.sh \
  scripts/vet-tu39.sh

git diff --cached --check
git status --short
