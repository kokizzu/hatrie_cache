#!/usr/bin/env bash
set -euo pipefail

paths=(
	Makefile
	README.md
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	CHG14_PREPARED_PLAN_METRICS.md
	hat/hatSql/query.go
	hat/hatSql/prepared_cache_key.go
	hat/hatSql/chg14_plan_cache_metrics_test.go
	scripts/benchmark-chg14-before.sh
	scripts/benchmark-chg14-after.sh
	scripts/format-chg14.sh
	scripts/race-chg14.sh
	scripts/vet-chg14.sh
	scripts/test-chg14-package.sh
	scripts/review-chg14.sh
	scripts/commit-chg14.sh
)

printf '%s\n' 'worktree status:'
git status --short --untracked-files=all
printf '%s\n' 'diff check:'
git diff --check -- "${paths[@]}"
printf '%s\n' 'format check:'
gofmt -d hat/hatSql/query.go hat/hatSql/prepared_cache_key.go hat/hatSql/chg14_plan_cache_metrics_test.go
printf '%s\n' 'reviewed paths:'
printf '%s\n' "${paths[@]}"
