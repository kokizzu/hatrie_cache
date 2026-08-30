#!/usr/bin/env sh
set -eu

git add -- \
  BENCHMARK.md \
  Makefile \
  hat/hatCache/sql_typed_index_baseline_benchmark_test.go \
  scripts/bench-sql-index-freshness-identity.sh \
  scripts/deliver-sql-index-freshness-benchmark.sh
git diff --cached --check
git commit -m "test(sql): measure index freshness identity"
git push
