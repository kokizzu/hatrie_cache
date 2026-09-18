#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  Makefile \
  README.md \
  TR037_DEADLOCK_DETECTION.md \
  hat/hatSql/tr037_deadlock_detection_benchmark_test.go \
  hat/hatSql/tr037_deadlock_detection_public_test.go \
  hat/hatSql/tr037_deadlock_detection_test.go \
  hat/hatSql/tt049_row_locks.go \
  scripts/benchmark-tr037-deadlock-detection.sh \
  scripts/commit-tr037-deadlock-detection.sh \
  scripts/format-tr037-deadlock-detection.sh \
  scripts/push-tr037-deadlock-detection.sh \
  scripts/race-tr037-deadlock-detection.sh \
  scripts/review-tr037.sh \
  scripts/stage-tr037-deadlock-detection.sh \
  scripts/test-tr037-deadlock-detection.sh \
  scripts/test-tr037-package.sh \
  scripts/vet-tr037-deadlock-detection.sh
