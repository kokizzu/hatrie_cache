#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	Makefile \
	MZ045_WORKLOAD_PLAN_EQUIVALENCE.md \
	hat/hatSql/compiled.go \
	hat/hatSql/mz024_arrangement_selection.go \
	hat/hatSql/mz045_arrangement_reuse_test.go \
	hat/hatSql/query.go \
	scripts/commit-mz045-arrangement-workload.sh \
	scripts/mz045-arrangement-workload.sh \
	scripts/push-mz045-arrangement-workload.sh \
	scripts/stage-mz045-arrangement-workload.sh
