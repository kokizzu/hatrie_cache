#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M241_OPTIMIZER_TRACE.md \
  Makefile \
  hat/hatSql/model.go \
  hat/hatSql/optimizer_rules.go \
  hat/hatSql/query.go \
  hat/hatSql/m241_optimizer_trace_test.go \
  hat/hatSql/m241_optimizer_trace_benchmark_test.go \
  hat/hatSql/m241_optimizer_trace_current_benchmark_test.go \
  scripts/benchmark-m241.sh \
  scripts/commit-m241.sh \
  scripts/push-m241.sh \
  scripts/stage-m241.sh \
  scripts/test-m241.sh

git diff --cached --check
git diff --cached --stat
