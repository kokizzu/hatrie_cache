#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	BENCHMARK.md \
	C234_QUERY_STAGE_PROFILER.md \
	INSPIRATION_ROUND2.md \
	hat/hatSql/query_profiler.go \
	hat/hatSql/ch234_stage_profiler.go \
	hat/hatSql/ch234_stage_profiler_test.go \
	hat/hatSql/ch234_stage_profiler_benchmark_test.go \
	scripts/benchmark-c234-baseline.sh \
	scripts/benchmark-c234-stage-profiler.sh \
	scripts/commit-c234-stage-profiler.sh \
	scripts/format-c234-stage-profiler.sh \
	scripts/push-c234-stage-profiler.sh \
	scripts/race-c234-stage-profiler.sh \
	scripts/stage-c234-stage-profiler.sh \
	scripts/test-c234-stage-profiler.sh \
	scripts/vet-c234-stage-profiler.sh

git status --short
