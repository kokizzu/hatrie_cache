#!/usr/bin/env bash
set -euo pipefail

git add Makefile hat/hatBackup/tu09_snapshot_join.go hat/hatBackup/tu09_snapshot_join_test.go hat/hatBackup/tu09_snapshot_join_benchmark_test.go scripts/format-tu09.sh scripts/test-tu09-red.sh scripts/test-tu09.sh scripts/test-tu09-package.sh scripts/benchmark-tu09.sh scripts/race-tu09.sh scripts/vet-tu09.sh scripts/review-tu09.sh scripts/stage-tu09.sh scripts/commit-tu09.sh scripts/push-tu09.sh
git add \
  TU09_SNAPSHOT_JOIN_BOOTSTRAP.md \
  PRODUCT_IDEA_GAPS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  README.md
