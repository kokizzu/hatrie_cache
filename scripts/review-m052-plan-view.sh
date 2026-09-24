#!/usr/bin/env bash
set -euo pipefail
git status --short
git diff --check
git diff --stat -- \
	BENCHMARK.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	M052_REUSABLE_DATAFLOW_PLAN_VIEW.md \
	hat/hatSql/m052_reusable_dataflow_plan_view.go \
	hat/hatSql/m052_reusable_dataflow_plan_view_test.go \
	hat/hatSql/m052_reusable_dataflow_plan_view_benchmark_test.go \
	Makefile \
	scripts/test-m052-plan-view.sh \
	scripts/format-m052-plan-view.sh \
	scripts/benchmark-m052-plan-view.sh \
	scripts/test-m052-plan-view-package.sh \
	scripts/race-m052-plan-view.sh \
	scripts/vet-m052-plan-view.sh
git diff -- \
	BENCHMARK.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	M052_REUSABLE_DATAFLOW_PLAN_VIEW.md \
	hat/hatSql/m052_reusable_dataflow_plan_view.go \
	hat/hatSql/m052_reusable_dataflow_plan_view_test.go \
	hat/hatSql/m052_reusable_dataflow_plan_view_benchmark_test.go \
	Makefile \
	scripts/test-m052-plan-view.sh \
	scripts/format-m052-plan-view.sh \
	scripts/benchmark-m052-plan-view.sh \
	scripts/test-m052-plan-view-package.sh \
	scripts/race-m052-plan-view.sh \
	scripts/vet-m052-plan-view.sh
