#!/bin/sh
set -eu

git diff --check
git add \
	Makefile \
	ENGINE_IDEAS.md \
	INSPIRATION.md \
	README.md \
	BENCHMARK.md \
	MZ045_COMPILED_PLAN_EQUIVALENCE.md \
	hat/hatSql/c213_compiled_plan_cache.go \
	hat/hatSql/query.go \
	hat/hatSql/mz045_plan_equivalence_test.go \
	hat/hatSql/mz045_explain_workload_test.go \
	scripts/benchmark-mz045-explain-workload.sh \
	scripts/test-mz045.sh \
	scripts/stage-mz045.sh \
	scripts/commit-mz045.sh \
	scripts/push-mz045.sh
git diff --cached --check
git diff --cached --name-only
