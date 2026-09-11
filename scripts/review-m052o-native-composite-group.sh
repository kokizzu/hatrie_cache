#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  INSPIRATION.md
  SQL_DATAFLOW_EXECUTOR.md
  hat/hatSql/m052c_native_dataflow.go
  hat/hatSql/m052o_native_composite_group_test.go
  hat/hatSql/m052o_native_composite_group_benchmark_test.go
  scripts/benchmark-m052o-native-composite-group-baseline.sh
  scripts/benchmark-m052o-native-composite-group.sh
  scripts/commit-m052o-native-composite-group.sh
  scripts/format-m052o-native-composite-group.sh
  scripts/push-m052o-native-composite-group.sh
  scripts/review-m052o-native-composite-group.sh
  scripts/test-m052o-native-composite-group.sh
  scripts/test-race-m052o-native-composite-group.sh
  scripts/vet-m052o-native-composite-group.sh
)

git diff --check -- "${paths[@]}"
git diff --stat -- "${paths[@]}"
git status --short -- "${paths[@]}"
