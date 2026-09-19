#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
  BENCHMARK.md \
  CHU34_SQL_MUTATION_IDEMPOTENCY.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatCache/chu34_sql_mutation_benchmark_test.go \
  hat/hatCache/chu34_sql_mutation_http_test.go \
  hat/hatCache/journal.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/sql.go \
  hat/hatSql/model.go \
  scripts/benchmark-chu34.sh \
  scripts/format-chu34.sh \
  scripts/race-chu34.sh \
  scripts/test-chu34-package.sh \
  scripts/test-chu34.sh \
  scripts/verify-chu34.sh \
  scripts/vet-chu34.sh
