#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  BENCHMARK.md \
  PRODUCT_IDEA_GAPS.md \
  TU09_JOIN_BOOTSTRAP.md \
  hat/hatReplication/join_bootstrap.go \
  hat/hatReplication/t_u09_join_bootstrap_test.go \
  hat/hatReplication/t_u09_join_bootstrap_benchmark_test.go \
  scripts/baseline-t-u09.sh \
  scripts/test-t-u09.sh \
  scripts/format-t-u09.sh \
  scripts/benchmark-t-u09.sh \
  scripts/test-t-u09-package.sh \
  scripts/race-t-u09.sh \
  scripts/vet-t-u09.sh \
  scripts/review-t-u09.sh \
  scripts/stage-t-u09.sh \
  scripts/commit-t-u09.sh \
  scripts/push-t-u09.sh
