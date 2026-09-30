#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add -- \
  BENCHMARK.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  SQL_IMPROVEMENTS_100.md \
  CHU61_NULLABLE_PREDICATE.md \
  hat/hatSql/columnar_null_predicate.go \
  hat/hatSql/query.go \
  hat/hatSql/chu61_nullable_predicate_test.go \
  hat/hatSql/chu61_nullable_predicate_benchmark_test.go \
  scripts/format-chu61-nullable-predicate.sh \
  scripts/test-chu61-nullable-predicate.sh \
  scripts/benchmark-chu61-nullable-predicate.sh \
  scripts/race-chu61-nullable-predicate.sh \
  scripts/review-chu61-nullable-predicate.sh \
  scripts/ship-chu61-nullable-predicate.sh \
  scripts/vet-chu61-nullable-predicate.sh \
  scripts/test-chu61-sql-package.sh \
  Makefile
git diff --cached --check
git commit -m 'chore(dev): keep CHU61 workflow targets [skip ci]'
git push -u origin HEAD
