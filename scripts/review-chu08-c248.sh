#!/usr/bin/env bash
set -euo pipefail

paths=(
	Makefile
	README.md
	PRODUCT_IDEA_GAPS.md
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	CHU08_AUTOMATIC_SQL_RESULT_CACHE.md
	hat/hatCache/main.go
	hat/hatCache/sql_query.go
	hat/hatCache/sql_result_cache_auto.go
	hat/hatCache/ch008_auto_result_cache_test.go
	hat/hatCache/ch008_auto_result_cache_benchmark_test.go
)

git diff --check -- "${paths[@]}"
printf '%s\n' '--- worktree status ---'
git status --short
printf '%s\n' '--- CH-U08 change stat ---'
git diff --stat -- "${paths[@]}"
printf '%s\n' '--- CH-U08 staged stat ---'
git diff --cached --stat -- "${paths[@]}"
