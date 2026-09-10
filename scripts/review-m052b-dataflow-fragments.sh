#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat -- \
  Makefile \
  BENCHMARK.md \
  COMPILED_DATAFLOW_IR.md \
  INSPIRATION.md \
  README.md \
  SQL_DATAFLOW_EXECUTOR.md \
  SQL_DATAFLOW_LOWERING.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  hat/hatSql/compiled_ir.go \
  hat/hatSql/dataflow_executor.go \
  hat/hatSql/m052b_dataflow_fragment_benchmark_test.go \
  hat/hatSql/m052b_dataflow_fragment_example_test.go \
  hat/hatSql/m052b_dataflow_fragment_test.go \
  scripts/benchmark-m052b-dataflow-fragments.sh \
  scripts/format-m052b-dataflow-fragments.sh \
  scripts/review-m052b-dataflow-fragments.sh \
  scripts/test-m052b-dataflow-fragments.sh \
  scripts/test-race-m052b-dataflow-fragments.sh \
  scripts/vet-m052b-dataflow-fragments.sh
