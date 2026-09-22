#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M240_EXPLAIN_DATAFLOW.md \
  Makefile \
  hat/hatSql/explain_dataflow.go \
  hat/hatSql/m240_dataflow_exchange_test.go \
  hat/hatSql/m240_dataflow_exchange_benchmark_test.go \
  scripts/benchmark-m240-dataflow.sh \
  scripts/commit-m240-dataflow.sh \
  scripts/push-m240-dataflow.sh \
  scripts/stage-m240-dataflow.sh \
  scripts/test-m240-dataflow.sh

git diff --cached --check
git diff --cached --stat
