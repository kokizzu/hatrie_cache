#!/usr/bin/env bash
set -euo pipefail

git add scripts/inspect-native-dataflow.sh

git add \
  SQL_AUTO_NATIVE_COMPOSITE_GROUPED_ORDERED.md \
  BENCHMARK.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION.md \
  hat/hatSql/m052c_native_dataflow.go \
  hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/m052y_auto_native_composite_grouped_ordered_test.go \
  hat/hatSql/m052y_auto_native_composite_grouped_ordered_benchmark_test.go \
  scripts/test-m052y-auto-native-composite-grouped-ordered.sh \
  scripts/benchmark-m052y-auto-native-composite-grouped-ordered.sh \
  scripts/format-m052y-auto-native-composite-grouped-ordered.sh \
  scripts/test-race-m052y-auto-native-composite-grouped-ordered.sh \
  scripts/vet-m052y-auto-native-composite-grouped-ordered.sh \
  scripts/review-m052y-auto-native-composite-grouped-ordered.sh \
  scripts/commit-m052y-auto-native-composite-grouped-ordered.sh \
  scripts/push-m052y-auto-native-composite-grouped-ordered.sh \
  Makefile
git commit -m "feat: auto-select native composite grouped top-n"
