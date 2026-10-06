#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	C235_TASK_PROFILER.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	hat/hatSql/c235_task_profiler_baseline_benchmark_test.go \
	hat/hatSql/c235_task_profiler_benchmark_test.go \
	hat/hatSql/c235_task_profiler_test.go \
	hat/hatSql/c235_task_profiler_validation_test.go \
	hat/hatSql/task_profiler.go \
	scripts/benchmark-c235-task-profiler-baseline.sh \
	scripts/benchmark-c235-task-profiler.sh \
	scripts/commit-c235-task-profiler.sh \
	scripts/format-c235-task-profiler.sh \
	scripts/push-c235-task-profiler.sh \
	scripts/race-c235-task-profiler.sh \
	scripts/test-c235-task-profiler.sh \
	scripts/verify-c235-task-profiler.sh \
	scripts/vet-c235-task-profiler.sh

git diff --cached --check
git commit -m "feat: add table-part task profiler [skip ci]"
