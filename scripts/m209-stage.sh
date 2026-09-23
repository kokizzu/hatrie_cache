#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M209_LOGICAL_FRONTIERS.md \
  Makefile \
  README.md \
  hat/hatSql/logical_frontier.go \
  hat/hatSql/m209_logical_frontier_benchmark_test.go \
  hat/hatSql/m209_logical_frontier_test.go \
  hat/hatSql/query.go \
  hat/hatSql/sql_snapshot_token.go \
  hat/hatSql/subscription.go \
  scripts/m209-benchmark.sh \
  scripts/m209-commit.sh \
  scripts/m209-format.sh \
  scripts/m209-push.sh \
  scripts/m209-race.sh \
  scripts/m209-stage.sh \
  scripts/m209-test.sh \
  scripts/m209-vet.sh

git diff --cached --check
git status --short
git diff --cached --stat
