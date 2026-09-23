#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M211_AS_OF_BOUNDS.md \
  Makefile \
  README.md \
  hat/hatSql/m211_as_of_bounds.go \
  hat/hatSql/m211_as_of_bounds_benchmark_test.go \
  hat/hatSql/m211_as_of_bounds_test.go \
  hat/hatSql/query.go \
  hat/hatSql/sql_snapshot_token.go \
  scripts/m211-benchmark.sh \
  scripts/m211-commit.sh \
  scripts/m211-format.sh \
  scripts/m211-push.sh \
  scripts/m211-race.sh \
  scripts/m211-stage.sh \
  scripts/m211-test.sh \
  scripts/m211-vet.sh

git diff --cached --check
git status --short
git diff --cached --stat
