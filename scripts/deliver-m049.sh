#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  M049_COMPILED_PLAN_SINGLEFLIGHT.md \
  Makefile \
  hat/hatSql/c213_compiled_plan_cache.go \
  hat/hatSql/m049_compiled_query_cache_singleflight_test.go \
  scripts/benchmark-m049-compiled-cache.sh \
  scripts/deliver-m049.sh \
  scripts/diffcheck-m049.sh \
  scripts/format-m049-compiled-cache.sh \
  scripts/race-m049-sql-package.sh \
  scripts/test-m049-all.sh \
  scripts/test-m049-compiled-cache.sh \
  scripts/test-m049-sql-package.sh \
  scripts/verify-m049-docs.sh \
  scripts/vet-m049-sql-package.sh
git diff --cached --check
git commit -m "perf(sql): coalesce compiled plan misses"
git push origin HEAD:master
