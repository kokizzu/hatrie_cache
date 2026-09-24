#!/usr/bin/env bash
set -euo pipefail
git add -- \
	BENCHMARK.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	M052_REUSABLE_DATAFLOW_PLAN_VIEW.md \
	Makefile \
	hat/hatSql/m052_reusable_dataflow_plan_view.go \
	hat/hatSql/m052_reusable_dataflow_plan_view_test.go \
	hat/hatSql/m052_reusable_dataflow_plan_view_benchmark_test.go \
	scripts/benchmark-m052-plan-view.sh \
	scripts/commit-m052-plan-view.sh \
	scripts/format-m052-plan-view.sh \
	scripts/race-m052-plan-view.sh \
	scripts/review-m052-plan-view.sh \
	scripts/push-m052-plan-view.sh \
	scripts/stage-m052-plan-view.sh \
	scripts/test-m052-plan-view-package.sh \
	scripts/test-m052-plan-view.sh \
	scripts/vet-m052-plan-view.sh
git diff --cached --check
git status --short
