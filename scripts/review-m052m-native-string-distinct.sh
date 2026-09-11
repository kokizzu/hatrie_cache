#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  INSPIRATION.md
  SQL_DATAFLOW_EXECUTOR.md
  hat/hatSql/m052c_native_dataflow.go
  hat/hatSql/m052f_native_distinct_test.go
  hat/hatSql/m052m_native_string_distinct_test.go
  hat/hatSql/m052m_native_string_distinct_benchmark_test.go
  scripts/benchmark-m052m-native-string-distinct-baseline.sh
  scripts/benchmark-m052m-native-string-distinct.sh
  scripts/commit-m052m-native-string-distinct.sh
  scripts/format-m052m-native-string-distinct.sh
  scripts/push-m052m-native-string-distinct.sh
  scripts/review-m052m-native-string-distinct.sh
  scripts/test-m052m-native-string-distinct.sh
  scripts/test-race-m052m-native-string-distinct.sh
  scripts/vet-m052m-native-string-distinct.sh
)

git diff --check -- "${paths[@]}"
git diff --stat -- "${paths[@]}"
git status --short -- "${paths[@]}"
