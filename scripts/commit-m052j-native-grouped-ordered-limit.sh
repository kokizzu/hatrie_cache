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
  hat/hatSql/m052j_native_grouped_ordered_limit_benchmark_test.go
  scripts/benchmark-m052j-native-grouped-ordered-limit.sh
  scripts/commit-m052j-native-grouped-ordered-limit.sh
  scripts/format-m052j-native-grouped-ordered-limit.sh
  scripts/inspect-query-engine-backlog.sh
  scripts/push-m052j-native-grouped-ordered-limit.sh
  scripts/review-m052j-native-grouped-ordered-limit.sh
  scripts/test-m052j-native-grouped-ordered-limit.sh
  scripts/test-race-m052j-native-grouped-ordered-limit.sh
  scripts/vet-m052j-native-grouped-ordered-limit.sh
)

git add -- "${paths[@]}"
git diff --cached --check
git commit --only -m 'hatSql: add native grouped ordered limit dataflow' -- "${paths[@]}"
