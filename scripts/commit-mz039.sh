#!/bin/sh
set -eu

git add \
	BENCHMARK.md \
	INSPIRATION_BACKLOG.md \
	Makefile \
	MZ039_OPERATOR_YIELD.md \
	README.md \
	hat/hatSql/m052p_auto_native_dataflow.go \
	hat/hatSql/mz039_operator_yield_benchmark_test.go \
	hat/hatSql/mz039_operator_yield_test.go \
	hat/hatSql/query.go \
	scripts/benchmark-mz039-after.sh \
	scripts/benchmark-mz039-before.sh \
	scripts/commit-mz039.sh \
	scripts/format-mz039.sh \
	scripts/inspect-mz039-control.sh \
	scripts/inspect-mz039-core.sh \
	scripts/inspect-mz039-native.sh \
	scripts/inspect-mz039.sh \
	scripts/push-mz039.sh \
	scripts/race-mz039.sh \
	scripts/review-mz039.sh \
	scripts/test-mz039-full.sh \
	scripts/test-mz039.sh \
	scripts/vet-mz039.sh
git commit -m "feat: add operator yield budgets"
