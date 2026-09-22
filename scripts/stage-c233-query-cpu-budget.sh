#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	C233_QUERY_CPU_BUDGET.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	hat/hatSql/query.go \
	hat/hatSql/c233_cpu_time_linux.go \
	hat/hatSql/c233_cpu_time_other.go \
	hat/hatSql/c233_query_cpu_budget.go \
	hat/hatSql/c233_query_cpu_budget_test.go \
	scripts/benchmark-c233-query-cpu-budget.sh \
	scripts/format-c233-query-cpu-budget.sh \
	scripts/race-c233-query-cpu-budget.sh \
	scripts/test-c233-query-cpu-budget.sh \
	scripts/vet-c233-query-cpu-budget.sh \
	scripts/stage-c233-query-cpu-budget.sh \
	scripts/commit-c233-query-cpu-budget.sh \
	scripts/push-c233-query-cpu-budget.sh
git diff --cached --check
git diff --cached --name-only
