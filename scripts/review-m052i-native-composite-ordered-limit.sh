#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  SQL_DATAFLOW_EXECUTOR.md
  BENCHMARK.md
  INSPIRATION.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  hat/hatSql/m052c_native_dataflow.go
  hat/hatSql/m052h_native_ordered_limit_test.go
  hat/hatSql/m052i_native_composite_ordered_limit_test.go
  hat/hatSql/m052i_native_composite_ordered_limit_benchmark_test.go
  scripts/test-m052i-native-composite-ordered-limit.sh
  scripts/test-race-m052i-native-composite-ordered-limit.sh
  scripts/vet-m052i-native-composite-ordered-limit.sh
  scripts/benchmark-m052i-native-composite-ordered-limit.sh
  scripts/format-m052i-native-composite-ordered-limit.sh
  scripts/review-m052i-native-composite-ordered-limit.sh
  scripts/commit-m052i-native-composite-ordered-limit.sh
  scripts/push-m052i-native-composite-ordered-limit.sh
)

git diff --check -- "${paths[@]}"
git diff --stat -- "${paths[@]}"
git status --short -- "${paths[@]}"
