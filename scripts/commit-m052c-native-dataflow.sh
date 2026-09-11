#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION.md \
  Makefile \
  SQL_DATAFLOW_EXECUTOR.md \
  hat/hatSql/dataflow_executor.go \
  hat/hatSql/m052c_native_dataflow.go \
  hat/hatSql/m052c_native_dataflow_benchmark_test.go \
  hat/hatSql/m052c_native_dataflow_test.go \
  scripts/benchmark-m052c-native-dataflow.sh \
  scripts/commit-m052c-native-dataflow.sh \
  scripts/format-m052c-native-dataflow.sh \
  scripts/push-m052c-native-dataflow.sh \
  scripts/review-m052c-native-dataflow.sh \
  scripts/test-m052c-native-dataflow.sh \
  scripts/test-race-m052c-native-dataflow.sh \
  scripts/vet-m052c-native-dataflow.sh
git diff --cached --check
git commit -m "hatSql: add native dataflow batch execution"
