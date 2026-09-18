#!/usr/bin/env bash
set -eu

git add \
  BENCHMARK.md \
  CH043_ASOF_TEMPORAL_JOIN.md \
  INSPIRATION_BACKLOG.md \
  Makefile \
  README.md \
  hat/hatSql/ch043_asof_join.go \
  hat/hatSql/ch043_asof_join_test.go \
  hat/hatSql/temporal_analytics.go \
  scripts/benchmark-ch043-asof-join.sh \
  scripts/benchmark-ch043-baseline.sh \
  scripts/commit-ch043-asof-join.sh \
  scripts/format-ch043-asof-join.sh \
  scripts/push-ch043-asof-join.sh \
  scripts/race-ch043-asof-join.sh \
  scripts/review-ch043-asof-join.sh \
  scripts/stage-ch043-asof-join.sh \
  scripts/test-ch043-asof-join.sh \
  scripts/test-ch043-package.sh \
  scripts/verify-ch043-asof-join-docs.sh \
  scripts/vet-ch043-asof-join.sh
