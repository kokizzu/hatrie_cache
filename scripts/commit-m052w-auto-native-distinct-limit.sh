#!/usr/bin/env bash
set -euo pipefail

git add \
  SQL_AUTO_NATIVE_DISTINCT_LIMIT.md \
  BENCHMARK.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION.md \
  scripts/inspect-native-dataflow.sh \
  hat/hatSql/m052c_native_dataflow.go \
  hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/m052w_auto_native_distinct_limit_test.go \
  hat/hatSql/m052w_auto_native_distinct_limit_benchmark_test.go \
  scripts/test-m052w-auto-native-distinct-limit.sh \
  scripts/benchmark-m052w-auto-native-distinct-limit.sh \
  scripts/format-m052w-auto-native-distinct-limit.sh \
  scripts/test-race-m052w-auto-native-distinct-limit.sh \
  scripts/vet-m052w-auto-native-distinct-limit.sh \
  scripts/review-m052w-auto-native-distinct-limit.sh \
  scripts/commit-m052w-auto-native-distinct-limit.sh \
  scripts/push-m052w-auto-native-distinct-limit.sh \
  Makefile
git commit -m "feat: auto-select native distinct limits"
