#!/usr/bin/env sh
set -eu

git add -- \
  BENCHMARK.md \
  Makefile \
  hat/hatCache/sql_query.go \
  hat/hatCache/sql_secondary_index_source_test.go \
  hat/hatCache/sql_typed_index_baseline_benchmark_test.go \
  scripts/bench-sql-secondary-index-source.sh \
  scripts/deliver-sql-secondary-index-source.sh \
  scripts/format-sql-materialized-order.sh \
  scripts/inspect-sql-secondary-index-implementation.sh \
  scripts/inspect-sql-secondary-index-source.sh \
  scripts/inspect-sql-typed-index-benchmark.sh \
  scripts/test-sql-secondary-index-source.sh
git diff --cached --check
git commit -m "perf(sql): avoid copied secondary index sources"
git push
