#!/usr/bin/env bash
set -euo pipefail

paths=(
	BENCHMARK.md
	ENGINE_IDEAS.md
	MZ045_ARRANGEMENT_PLAN_CACHE.md
	Makefile
	hat/hatSql/mz045_arrangement_plan_cache.go
	hat/hatSql/mz045_arrangement_cache_test.go
	hat/hatSql/query.go
	scripts/benchmark-mz045-arrangement-cache.sh
	scripts/commit-mz045-arrangement-cache.sh
	scripts/format-mz045-arrangement-cache.sh
	scripts/push-mz045-arrangement-cache.sh
	scripts/stage-mz045-arrangement-cache.sh
	scripts/test-mz045-arrangement-cache.sh
	scripts/verify-mz045-arrangement-cache.sh
)

git diff --check -- "${paths[@]}"
git add -- "${paths[@]}"
git diff --cached --check
