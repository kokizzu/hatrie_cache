#!/usr/bin/env sh
set -eu

git add -- \
  BENCHMARK.md \
  Makefile \
  hat/hatCache/sql_index_value_key_benchmark_test.go \
  hat/hatCache/sql_query.go \
  scripts/bench-sql-index-value-key.sh \
  scripts/deliver-sql-index-value-key.sh \
  scripts/format-sql-materialized-order.sh \
  scripts/inspect-sql-direct-string-source.sh \
  scripts/test-sql-index-value-key.sh
git diff --cached --check
git commit -m "perf(sql): specialize numeric index key encoding"
git push
