#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CHU59_NUMERIC_BETWEEN.md \
  Makefile \
  SQL_IMPROVEMENTS_100.md \
  hat/hatSql/chu59_numeric_between_benchmark_test.go \
  hat/hatSql/chu59_numeric_between_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-chu59-numeric-between.sh \
  scripts/format-chu59-numeric-between.sh \
  scripts/race-chu59-numeric-between.sh \
  scripts/review-chu59-numeric-between.sh \
  scripts/test-chu59-numeric-between.sh \
  scripts/verify-chu59-numeric-between.sh \
  scripts/vet-chu59-numeric-between.sh \
  scripts/ship-chu59-numeric-between.sh

git diff --cached --check
git diff --cached --stat
git commit -m 'feat(sql): vectorize literal numeric BETWEEN [skip ci]'
git push -u origin HEAD
