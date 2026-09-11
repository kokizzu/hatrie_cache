#!/usr/bin/env bash
set -euo pipefail

git add \
	SQL_AUTO_NATIVE_OPERATORS.md \
	BENCHMARK.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION.md \
	hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/m052q_auto_native_operators_test.go \
	hat/hatSql/m052q_auto_native_operators_benchmark_test.go \
	scripts/inspect-native-dataflow.sh \
	scripts/test-m052q-auto-native-operators.sh \
  scripts/benchmark-m052q-auto-native-operators.sh \
  scripts/format-m052q-auto-native-operators.sh \
  scripts/test-race-m052q-auto-native-operators.sh \
  scripts/vet-m052q-auto-native-operators.sh \
  scripts/review-m052q-auto-native-operators.sh \
  scripts/commit-m052q-auto-native-operators.sh \
  scripts/push-m052q-auto-native-operators.sh \
  Makefile
git commit -m "feat: auto-select native aggregate and distinct SQL"
