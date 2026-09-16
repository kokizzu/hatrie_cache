#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --stat -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CHU09_PERSISTED_SQL_RESULT_CACHE.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatCache/sql_result_cache_auto.go \
  hat/hatCache/sql_result_cache_persistence_test.go \
  hat/hatSql/result_cache_persistence.go \
  hat/hatSql/result_cache_persistence_benchmark_test.go \
  hat/hatSql/result_cache_persistence_test.go \
  scripts/benchmark-chu09-c249.sh \
  scripts/format-chu09-c249.sh \
  scripts/race-chu09-c249.sh \
  scripts/test-chu09-c249.sh \
  scripts/test-chu09-hatcache-c249.sh \
  scripts/test-chu09-package-c249.sh \
  scripts/test-chu09-repo-c249.sh \
  scripts/stage-chu09-c249.sh \
  scripts/commit-chu09-c249.sh \
  scripts/push-chu09-c249.sh \
  scripts/verify-chu09-docs-c249.sh \
  scripts/vet-chu09-c249.sh
git status --short
