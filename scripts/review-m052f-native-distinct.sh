#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  SQL_DATAFLOW_EXECUTOR.md
  BENCHMARK.md
  INSPIRATION.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  hat/hatSql/m052c_native_dataflow.go
  hat/hatSql/m052f_native_distinct_test.go
  hat/hatSql/m052f_native_distinct_benchmark_test.go
  scripts/test-m052f-native-distinct.sh
  scripts/test-race-m052f-native-distinct.sh
  scripts/vet-m052f-native-distinct.sh
  scripts/benchmark-m052f-native-distinct.sh
  scripts/format-m052f-native-distinct.sh
  scripts/review-m052f-native-distinct.sh
  scripts/commit-m052f-native-distinct.sh
  scripts/push-m052f-native-distinct.sh
)

git diff --check -- "${paths[@]}"
git diff --stat -- "${paths[@]}"
git status --short -- "${paths[@]}"
