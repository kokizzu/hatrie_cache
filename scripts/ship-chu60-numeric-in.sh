#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CHU60_NUMERIC_IN.md \
  Makefile \
  SQL_IMPROVEMENTS_100.md \
  hat/hatSql/chu60_numeric_in_benchmark_test.go \
  hat/hatSql/chu60_numeric_in_test.go \
  hat/hatSql/columnar_numeric_predicate.go \
  hat/hatSql/query.go \
  scripts/benchmark-chu60-numeric-in.sh \
  scripts/format-chu60-numeric-in.sh \
  scripts/race-chu60-numeric-in.sh \
  scripts/review-chu60-numeric-in.sh \
  scripts/ship-chu60-numeric-in.sh \
  scripts/test-chu60-numeric-in.sh \
  scripts/verify-chu60-numeric-in.sh \
  scripts/vet-chu60-numeric-in.sh

git diff --cached --check
git diff --cached --stat
git commit -m 'feat(sql): vectorize numeric IN literals [skip ci]'
git push -u origin HEAD
