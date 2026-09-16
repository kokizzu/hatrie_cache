#!/usr/bin/env bash
set -euo pipefail

paths=(
	Makefile
	README.md
	ENGINE_IDEAS.md
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	SQL_PLANNER_STATISTICS.md
	hat/hatCache/sql_planner_statistics_persistence.go
	hat/hatCache/sql_planner_statistics_test.go
	hat/hatCache/tt050_sql_planner_statistics_persistence_benchmark_test.go
	scripts/benchmark-before-tt050-durable-stats-c292.sh
	scripts/benchmark-tt050-durable-stats-c292.sh
	scripts/format-tt050-durable-stats-c292.sh
	scripts/test-tt050-durable-stats-c292.sh
	scripts/verify-tt050-durable-stats-c292.sh
	scripts/review-tt050-durable-stats-c292.sh
	scripts/stage-tt050-durable-stats-c292.sh
	scripts/inspect-staged-tt050-durable-stats-c292.sh
	scripts/commit-tt050-durable-stats-c292.sh
	scripts/push-tt050-durable-stats-c292.sh
)

if ! git diff --cached --quiet; then
	echo "refusing review with pre-existing staged changes" >&2
	exit 1
fi

for path in "${paths[@]}"; do
	if [[ ! -e "$path" ]]; then
		echo "missing expected TT-050 path: $path" >&2
		exit 1
	fi
done

check_paths=(
	README.md
	ENGINE_IDEAS.md
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	SQL_PLANNER_STATISTICS.md
	hat/hatCache/sql_planner_statistics_persistence.go
	hat/hatCache/sql_planner_statistics_test.go
	hat/hatCache/tt050_sql_planner_statistics_persistence_benchmark_test.go
	scripts/benchmark-before-tt050-durable-stats-c292.sh
	scripts/benchmark-tt050-durable-stats-c292.sh
	scripts/format-tt050-durable-stats-c292.sh
	scripts/test-tt050-durable-stats-c292.sh
	scripts/verify-tt050-durable-stats-c292.sh
	scripts/review-tt050-durable-stats-c292.sh
	scripts/stage-tt050-durable-stats-c292.sh
	scripts/inspect-staged-tt050-durable-stats-c292.sh
	scripts/commit-tt050-durable-stats-c292.sh
	scripts/push-tt050-durable-stats-c292.sh
)
git diff --check -- "${check_paths[@]}"
printf '%s\n' '--- TT-050 working-tree status ---'
git status --short -- "${paths[@]}"
printf '%s\n' '--- TT-050 working-tree diff stat ---'
git diff --stat -- "${check_paths[@]}"
