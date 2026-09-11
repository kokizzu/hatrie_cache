#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  INSPIRATION.md
  SQL_DATAFLOW_EXECUTOR.md
  hat/hatSql/m052c_native_dataflow.go
  hat/hatSql/m052j_native_grouped_ordered_limit_test.go
  hat/hatSql/m052k_native_grouped_having_test.go
  hat/hatSql/m052k_native_grouped_having_benchmark_test.go
  scripts/benchmark-m052k-native-grouped-having.sh
  scripts/commit-m052k-native-grouped-having.sh
  scripts/format-m052k-native-grouped-having.sh
  scripts/inspect-query-engine-backlog.sh
  scripts/push-m052k-native-grouped-having.sh
  scripts/review-m052k-native-grouped-having.sh
  scripts/test-m052k-native-grouped-having.sh
  scripts/test-race-m052k-native-grouped-having.sh
  scripts/vet-m052k-native-grouped-having.sh
)

git diff --check -- "${paths[@]}"
git diff --stat -- "${paths[@]}"
git status --short -- "${paths[@]}"
