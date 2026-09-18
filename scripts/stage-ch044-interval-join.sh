#!/usr/bin/env bash
set -eu

git add Makefile \
  BENCHMARK.md \
  CH044_INTERVAL_JOIN_MAINTENANCE.md \
  INSPIRATION_BACKLOG.md \
  README.md \
  hat/hatSql/ch044_interval_join_benchmark_test.go \
  hat/hatSql/m029_incremental_interval_join.go \
  scripts/benchmark-ch044-interval-join.sh \
  scripts/commit-ch044-interval-join.sh \
  scripts/format-ch044-interval-join.sh \
  scripts/push-ch044-interval-join.sh \
  scripts/race-ch044-interval-join.sh \
  scripts/review-ch044-interval-join.sh \
  scripts/stage-ch044-interval-join.sh \
  scripts/test-ch044-interval-join.sh \
  scripts/test-ch044-package.sh \
  scripts/verify-ch044-interval-join-docs.sh \
  scripts/vet-ch044-interval-join.sh
