#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  TU53_LOGICAL_TIME_FRONTIER.md \
  hat/hatDataStructure/logical_frontier.go \
  hat/hatDataStructure/tu53_frontier_benchmark_test.go \
  hat/hatDataStructure/tu53_frontier_test.go \
  scripts/benchmark-tu53-frontier.sh \
  scripts/commit-tu53-frontier.sh \
  scripts/format-tu53-frontier.sh \
  scripts/push-tu53-frontier.sh \
  scripts/race-tu53-frontier.sh \
  scripts/review-tu53-frontier.sh \
  scripts/stage-tu53-frontier.sh \
  scripts/test-tu53-frontier-package.sh \
  scripts/test-tu53-frontier.sh \
  scripts/vet-tu53-frontier.sh
