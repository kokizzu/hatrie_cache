#!/usr/bin/env bash
set -euo pipefail

git add \
  SQL_AUTO_NATIVE_SCALAR_LIMIT.md \
  BENCHMARK.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION.md \
  scripts/inspect-native-dataflow.sh \
  hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/m052v_auto_native_scalar_limit_test.go \
  hat/hatSql/m052v_auto_native_scalar_limit_benchmark_test.go \
  scripts/test-m052v-auto-native-scalar-limit.sh \
  scripts/benchmark-m052v-auto-native-scalar-limit.sh \
  scripts/format-m052v-auto-native-scalar-limit.sh \
  scripts/test-race-m052v-auto-native-scalar-limit.sh \
  scripts/vet-m052v-auto-native-scalar-limit.sh \
  scripts/review-m052v-auto-native-scalar-limit.sh \
  scripts/commit-m052v-auto-native-scalar-limit.sh \
  scripts/push-m052v-auto-native-scalar-limit.sh \
  Makefile
git commit -m "feat: auto-select native scalar limits"
