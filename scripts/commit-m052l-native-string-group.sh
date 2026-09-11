#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  INSPIRATION.md
  SQL_DATAFLOW_EXECUTOR.md
  hat/hatSql/m052c_native_dataflow.go
  hat/hatSql/m052e_native_group_test.go
  hat/hatSql/m052l_native_string_group_test.go
  hat/hatSql/m052l_native_string_group_benchmark_test.go
  scripts/benchmark-m052l-native-string-group-baseline.sh
  scripts/benchmark-m052l-native-string-group.sh
  scripts/commit-m052l-native-string-group.sh
  scripts/format-m052l-native-string-group.sh
  scripts/push-m052l-native-string-group.sh
  scripts/review-m052l-native-string-group.sh
  scripts/test-m052l-native-string-group.sh
  scripts/test-race-m052l-native-string-group.sh
  scripts/vet-m052l-native-string-group.sh
)

git add -- "${paths[@]}"
git diff --cached --check
git commit --only -m 'hatSql: add native string grouped dataflow' -- "${paths[@]}"
