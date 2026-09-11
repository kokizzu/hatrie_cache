#!/usr/bin/env bash
set -euo pipefail

git add \
  SQL_AUTO_NATIVE_AGGREGATE_LIMIT.md \
  BENCHMARK.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION.md \
  scripts/inspect-native-dataflow.sh \
  hat/hatSql/m052c_native_dataflow.go \
  hat/hatSql/m052g_native_limit_test.go \
  hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/m052x_auto_native_aggregate_limit_test.go \
  hat/hatSql/m052x_auto_native_aggregate_limit_benchmark_test.go \
  scripts/test-m052x-auto-native-aggregate-limit.sh \
  scripts/benchmark-m052x-auto-native-aggregate-limit.sh \
  scripts/format-m052x-auto-native-aggregate-limit.sh \
  scripts/test-race-m052x-auto-native-aggregate-limit.sh \
  scripts/vet-m052x-auto-native-aggregate-limit.sh \
  scripts/review-m052x-auto-native-aggregate-limit.sh \
  scripts/commit-m052x-auto-native-aggregate-limit.sh \
  scripts/push-m052x-auto-native-aggregate-limit.sh \
  Makefile
git commit -m "feat: auto-select native aggregate limits"
