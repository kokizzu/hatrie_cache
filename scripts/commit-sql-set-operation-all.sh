#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION.md \
  Makefile \
  SQL_SET_OPERATIONS.md \
  DIFFERENTIAL_OPERATORS.md \
  hat/hatCache/sql_function_test.go \
  hat/hatSql/query.go \
  hat/hatSql/sql_set_operation_all_benchmark_test.go \
  hat/hatSql/sql_set_operation_all_test.go \
  scripts/benchmark-sql-set-operation-all.sh \
  scripts/commit-sql-set-operation-all.sh \
  scripts/format-sql-set-operation-all.sh \
  scripts/test-sql-set-operation-all.sh \
  scripts/verify-sql-set-operation-all.sh
git diff --cached --check
git diff --cached --stat
git commit -m "hatSql: support INTERSECT ALL and EXCEPT ALL"
git push origin HEAD:master
