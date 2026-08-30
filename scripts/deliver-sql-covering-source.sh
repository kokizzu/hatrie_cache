#!/usr/bin/env sh
set -eu

git add -- \
  BENCHMARK.md \
  Makefile \
  hat/hatCache/sql_covering_index_test.go \
  hat/hatCache/sql_query.go \
  hat/hatCache/sql_typed_index_baseline_benchmark_test.go \
  scripts/deliver-sql-covering-source.sh \
  scripts/format-sql-materialized-order.sh \
  scripts/inspect-covering-source-benchmark.sh \
  scripts/test-sql-index-source-snapshot.sh
git diff --cached --check
git commit -m "perf(sql): avoid copied covering index sources"
git push
