#!/usr/bin/env bash
set -euo pipefail

git add \
  SQL_AUTO_NATIVE_GROUPED_ORDERED.md \
  BENCHMARK.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION.md \
  scripts/inspect-native-dataflow.sh \
  hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/m052t_auto_native_grouped_ordered_test.go \
  hat/hatSql/m052t_auto_native_grouped_ordered_benchmark_test.go \
  scripts/test-m052t-auto-native-grouped-ordered.sh \
  scripts/benchmark-m052t-auto-native-grouped-ordered.sh \
  scripts/format-m052t-auto-native-grouped-ordered.sh \
  scripts/test-race-m052t-auto-native-grouped-ordered.sh \
  scripts/vet-m052t-auto-native-grouped-ordered.sh \
  scripts/review-m052t-auto-native-grouped-ordered.sh \
  scripts/commit-m052t-auto-native-grouped-ordered.sh \
  scripts/push-m052t-auto-native-grouped-ordered.sh \
  Makefile
git commit -m "feat: auto-select native grouped top-n SQL"
