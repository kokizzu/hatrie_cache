#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	C233_CPU_TIME_BUDGET.md \
	INSPIRATION_ROUND2.md \
	hat/hatSql/ch233_cpu_budget.go \
	hat/hatSql/ch233_cpu_budget_benchmark_test.go \
	hat/hatSql/ch233_cpu_budget_test.go \
	hat/hatSql/ch233_cpu_clock_linux.go \
	hat/hatSql/ch233_cpu_clock_other.go \
	hat/hatSql/query.go \
	scripts/benchmark-c233-baseline.sh \
	scripts/benchmark-c233-cpu-budget.sh \
	scripts/format-c233-cpu-budget.sh \
	scripts/race-c233-package.sh \
	scripts/race-c233-cpu-budget.sh \
	scripts/stage-c233-cpu-budget.sh \
	scripts/test-c233-package.sh \
	scripts/test-c233-cpu-budget.sh \
	scripts/vet-c233-cpu-budget.sh \
	scripts/commit-c233-cpu-budget.sh \
	scripts/push-c233-cpu-budget.sh

git status --short
