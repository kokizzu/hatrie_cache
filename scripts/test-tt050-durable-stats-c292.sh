#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(pwd)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
git archive HEAD | tar -x -C "$tmp_dir"
cp "$repo_dir/hat/hatCache/sql_planner_statistics.go" "$tmp_dir/hat/hatCache/sql_planner_statistics.go"
cp "$repo_dir/hat/hatCache/sql_planner_statistics_test.go" "$tmp_dir/hat/hatCache/sql_planner_statistics_test.go"
if [[ -f "$repo_dir/hat/hatCache/sql_planner_statistics_persistence.go" ]]; then
  cp "$repo_dir/hat/hatCache/sql_planner_statistics_persistence.go" "$tmp_dir/hat/hatCache/sql_planner_statistics_persistence.go"
fi
cd "$tmp_dir"
go test ./hat/hatCache -run '^TestSQLPlannerStatisticsPersistence' -count=1 -v
