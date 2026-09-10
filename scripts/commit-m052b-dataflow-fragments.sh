#!/usr/bin/env bash
set -euo pipefail

git add -- \
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
  scripts/commit-m052b-dataflow-fragments.sh \
  scripts/format-m052b-dataflow-fragments.sh \
  scripts/push-m052b-dataflow-fragments.sh \
  scripts/review-m052b-dataflow-fragments.sh \
  scripts/test-m052b-dataflow-fragments.sh \
  scripts/test-race-m052b-dataflow-fragments.sh \
  scripts/vet-m052b-dataflow-fragments.sh
git diff --cached --check
git commit -m "feat(sql): add executable dataflow fragments"
