#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  README.md \
  SQL_QUERY_PROFILER.md \
  Makefile \
  hat/hatSql/query_profiler.go \
  hat/hatSql/query_profiler_test.go \
  hat/hatSql/query_profiler_benchmark_test.go \
  scripts/benchmark-ch032-query-profiler.sh \
  scripts/commit-ch032-query-profiler.sh \
  scripts/format-ch032-query-profiler.sh \
  scripts/push-ch032-query-profiler.sh \
  scripts/review-ch032-query-profiler.sh \
  scripts/test-ch032-query-profiler.sh \
  scripts/test-race-ch032-query-profiler.sh \
  scripts/verify-ch032-query-profiler-docs.sh
git commit -m 'sql: add bounded query profiler samples'
