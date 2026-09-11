#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md SQL_COMPUTE_STORAGE_SEPARATION.md \
  hat/hatSql/query_manager.go hat/hatSql/mz018_compute_pool_test.go \
  hat/hatSql/mz018_compute_pool_benchmark_test.go hat/hatCache/sql_query.go \
  scripts/benchmark-mz018-compute-pool.sh scripts/test-mz018-compute-pool.sh \
  scripts/format-mz018-compute-pool.sh scripts/test-race-mz018-compute-pool.sh \
  scripts/test-mz018-broad.sh scripts/verify-mz018-compute-pool-docs.sh \
  scripts/review-mz018-compute-pool.sh scripts/status-mz018-compute-pool.sh \
  scripts/commit-mz018-compute-pool.sh scripts/push-mz018-compute-pool.sh
git commit -m "adopt opt-in SQL compute pool"
