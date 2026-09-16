#!/bin/sh
set -eu

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C204_PROJECTION_IDEMPOTENCY.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  hat/hatCache/c204_projection_idempotency_test.go \
  hat/hatCache/sql_incremental_projection.go \
  hat/hatSql/c204_projection_idempotency_benchmark_test.go \
  hat/hatSql/incremental_projection.go \
  hat/hatSql/materialized.go \
  scripts/benchmark-chu04-c204.sh \
  scripts/commit-chu04-c204.sh \
  scripts/format-chu04-c204.sh \
  scripts/push-chu04-c204.sh \
  scripts/race-chu04-c204.sh \
  scripts/stage-chu04-c204.sh \
  scripts/test-chu04-c204-package.sh \
  scripts/test-chu04-c204.sh \
  scripts/vet-chu04-c204.sh

git diff --cached --check
git diff --cached --stat
