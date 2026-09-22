#!/usr/bin/env bash
set -euo pipefail
git add -- \
  BENCHMARK.md \
  C234_QUERY_PROFILER.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  SQL_QUERY_PROFILER.md \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  hat/hatSql/query_profiler.go \
  hat/hatSql/c234_query_profiler_baseline_benchmark_test.go \
  hat/hatSql/c234_query_profiler_test.go \
  scripts/benchmark-c234-query-profiler.sh \
  scripts/commit-c234-query-profiler.sh \
  scripts/format-c234-query-profiler.sh \
  scripts/push-c234-query-profiler.sh \
  scripts/race-c234-query-profiler.sh \
  scripts/stage-c234-query-profiler.sh \
  scripts/test-c234-query-profiler.sh \
  scripts/verify-c234-query-profiler.sh \
  scripts/vet-c234-query-profiler.sh
git diff --cached --check
git diff --cached --stat
