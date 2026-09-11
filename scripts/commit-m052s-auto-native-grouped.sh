#!/usr/bin/env bash
set -euo pipefail

git add \
  SQL_AUTO_NATIVE_GROUPED.md \
  BENCHMARK.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION.md \
  hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/m052p_auto_native_dataflow_test.go \
  hat/hatSql/m052q_auto_native_operators_test.go \
  hat/hatSql/m052s_auto_native_grouped_test.go \
  hat/hatSql/m052s_auto_native_grouped_benchmark_test.go \
  scripts/inspect-native-dataflow.sh \
  scripts/test-m052s-auto-native-grouped.sh \
  scripts/benchmark-m052s-auto-native-grouped.sh \
  scripts/format-m052s-auto-native-grouped.sh \
  scripts/test-race-m052s-auto-native-grouped.sh \
  scripts/vet-m052s-auto-native-grouped.sh \
  scripts/review-m052s-auto-native-grouped.sh \
  scripts/commit-m052s-auto-native-grouped.sh \
  scripts/push-m052s-auto-native-grouped.sh \
  Makefile
git commit -m "feat: auto-select native grouped SQL"
