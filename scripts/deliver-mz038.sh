#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C212_TYPED_TABLE_ORDER_CACHE.md \
  ENGINE_IDEAS.md \
  Makefile \
  hat/hatSql/c212_typed_table_order_benchmark_test.go \
  hat/hatSql/c212_typed_table_order_test.go \
  hat/hatSql/typed_table_columnar_order.go \
  scripts/benchmark-mz038.sh \
  scripts/deliver-mz038.sh \
  scripts/format-mz038.sh \
  scripts/race-mz038.sh \
  scripts/test-mz038-package.sh \
  scripts/test-mz038.sh \
  scripts/vet-mz038.sh
git diff --cached --check
git commit -m "perf(sql): cache composite typed-table orders"
git push origin HEAD:master
