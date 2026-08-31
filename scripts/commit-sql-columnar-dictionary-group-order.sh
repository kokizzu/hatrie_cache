#!/bin/sh
set -eu

git diff --check -- \
  BENCHMARK.md \
  Makefile \
  hat/hatCache/sql_columnar_dictionary_group_order_benchmark_test.go \
  hat/hatCache/sql_columnar_dictionary_group_order_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-sql-columnar-dictionary-group-order.sh \
  scripts/commit-sql-columnar-dictionary-group-order.sh \
  scripts/format-sql-columnar-dictionary-group-order.sh \
  scripts/test-sql-columnar-dictionary-group-order.sh
git add -- \
  BENCHMARK.md \
  Makefile \
  hat/hatCache/sql_columnar_dictionary_group_order_benchmark_test.go \
  hat/hatCache/sql_columnar_dictionary_group_order_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-sql-columnar-dictionary-group-order.sh \
  scripts/commit-sql-columnar-dictionary-group-order.sh \
  scripts/format-sql-columnar-dictionary-group-order.sh \
  scripts/test-sql-columnar-dictionary-group-order.sh
git commit --only -m 'perf: order SQL dictionary group aggregates directly' -- \
  BENCHMARK.md \
  Makefile \
  hat/hatCache/sql_columnar_dictionary_group_order_benchmark_test.go \
  hat/hatCache/sql_columnar_dictionary_group_order_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-sql-columnar-dictionary-group-order.sh \
  scripts/commit-sql-columnar-dictionary-group-order.sh \
  scripts/format-sql-columnar-dictionary-group-order.sh \
  scripts/test-sql-columnar-dictionary-group-order.sh
git push
