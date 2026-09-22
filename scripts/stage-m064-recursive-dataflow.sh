#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION.md \
  M064_RECURSIVE_DATAFLOW.md \
  Makefile \
  hat/hatSql/m064_recursive_dataflow.go \
  hat/hatSql/m064_recursive_dataflow_baseline_benchmark_test.go \
  hat/hatSql/m064_recursive_dataflow_benchmark_test.go \
  hat/hatSql/m064_recursive_dataflow_test.go \
  scripts/benchmark-m064-recursive-dataflow-before.sh \
  scripts/benchmark-m064-recursive-dataflow.sh \
  scripts/commit-m064-recursive-dataflow.sh \
  scripts/format-m064-recursive-dataflow.sh \
  scripts/push-m064-recursive-dataflow.sh \
  scripts/race-m064-recursive-dataflow.sh \
  scripts/stage-m064-recursive-dataflow.sh \
  scripts/status-m064-recursive-dataflow.sh \
  scripts/test-m064-recursive-dataflow.sh \
  scripts/test-m064-sql-package.sh \
  scripts/vet-m064-recursive-dataflow.sh
git diff --cached --check
