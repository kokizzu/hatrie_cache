#!/usr/bin/env bash
set -euo pipefail

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INSPIRATION.md \
	SQL_AUTO_NATIVE_DATAFLOW.md \
	hat/hatSql/m052p_auto_native_dataflow.go \
	hat/hatSql/m052p_auto_native_dataflow_test.go \
	hat/hatSql/m052p_auto_native_dataflow_benchmark_test.go \
	scripts/inspect-native-dataflow.sh \
	scripts/inspect-query-engine-backlog.sh \
	scripts/test-m052p-auto-native-dataflow.sh \
	scripts/benchmark-m052p-auto-native-dataflow.sh \
	scripts/format-m052p-auto-native-dataflow.sh \
	scripts/test-race-m052p-auto-native-dataflow.sh \
	scripts/vet-m052p-auto-native-dataflow.sh \
	scripts/review-m052p-auto-native-dataflow.sh \
	scripts/commit-m052p-auto-native-dataflow.sh \
	scripts/push-m052p-auto-native-dataflow.sh \
	Makefile
git commit -m "feat: auto-select safe native SQL dataflow"
