#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  SQL_DATAFLOW_EXECUTOR.md
  BENCHMARK.md
  INSPIRATION.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  hat/hatSql/m052c_native_dataflow.go
  hat/hatSql/m052g_native_limit_test.go
  hat/hatSql/m052g_native_limit_benchmark_test.go
  scripts/test-m052g-native-limit.sh
  scripts/test-race-m052g-native-limit.sh
  scripts/vet-m052g-native-limit.sh
  scripts/benchmark-m052g-native-limit.sh
  scripts/format-m052g-native-limit.sh
  scripts/review-m052g-native-limit.sh
  scripts/commit-m052g-native-limit.sh
  scripts/push-m052g-native-limit.sh
)

git add -- "${paths[@]}"
git diff --cached --check
git commit --only -m 'hatSql: add native limit dataflow' -- "${paths[@]}"
