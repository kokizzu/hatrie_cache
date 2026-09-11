#!/usr/bin/env bash
set -euo pipefail

git add \
  SQL_AUTO_NATIVE_ORDERED.md \
  BENCHMARK.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION.md \
  hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/m052r_auto_native_ordered_test.go \
  hat/hatSql/m052r_auto_native_ordered_benchmark_test.go \
  scripts/inspect-native-dataflow.sh \
  scripts/test-m052r-auto-native-ordered.sh \
  scripts/benchmark-m052r-auto-native-ordered.sh \
  scripts/format-m052r-auto-native-ordered.sh \
  scripts/test-race-m052r-auto-native-ordered.sh \
  scripts/vet-m052r-auto-native-ordered.sh \
  scripts/review-m052r-auto-native-ordered.sh \
  scripts/commit-m052r-auto-native-ordered.sh \
  scripts/push-m052r-auto-native-ordered.sh \
  Makefile
git commit -m "feat: auto-select native ordered SQL"
