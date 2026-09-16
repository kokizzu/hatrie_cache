#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(pwd)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
git archive HEAD | tar -x -C "$tmp_dir"
cp "$repo_dir/hat/hatCache/sql_planner_statistics_benchmark_test.go" "$tmp_dir/hat/hatCache/sql_planner_statistics_benchmark_test.go"
cd "$tmp_dir"
go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLPlannerStatisticsAnalyze$' -benchtime=200ms -benchmem -count=5
