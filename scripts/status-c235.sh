#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat -- \
  Makefile \
  README.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION_ROUND2.md \
  BENCHMARK.md \
  C235_TASK_PROFILER.md \
  hat/hatSql/task_profiler.go \
  hat/hatSql/c235_task_profiler_test.go \
  hat/hatSql/c235_task_profiler_benchmark_test.go \
  scripts/benchmark-c235.sh \
  scripts/test-c235.sh \
  scripts/verify-c235.sh \
  scripts/status-c235.sh \
  scripts/commit-c235.sh \
  scripts/push-c235.sh
