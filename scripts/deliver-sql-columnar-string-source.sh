#!/usr/bin/env sh
set -eu

git add -- \
  BENCHMARK.md \
  Makefile \
  hat/hatCache/sql_direct_string_source_benchmark_test.go \
  hat/hatCache/sql_direct_string_source_test.go \
  hat/hatCache/sql_query.go \
  scripts/bench-sql-columnar-string-source.sh \
  scripts/deliver-sql-columnar-string-source.sh \
  scripts/format-sql-materialized-order.sh \
  scripts/inspect-sql-direct-string-source.sh \
  scripts/test-sql-direct-string-source.sh
git diff --cached --check
git commit -m "perf(sql): avoid copied columnar string sources"
git push
