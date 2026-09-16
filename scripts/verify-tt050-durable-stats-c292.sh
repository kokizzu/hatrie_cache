#!/usr/bin/env bash
set -euo pipefail

workdir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-tt050-verify.XXXXXX")
trap 'rm -rf "$workdir"' EXIT

git archive HEAD | tar -x -C "$workdir"
cp hat/hatCache/sql_planner_statistics.go "$workdir/hat/hatCache/sql_planner_statistics.go"
cp hat/hatCache/sql_planner_statistics_persistence.go "$workdir/hat/hatCache/sql_planner_statistics_persistence.go"
cp hat/hatCache/sql_planner_statistics_test.go "$workdir/hat/hatCache/sql_planner_statistics_test.go"

(
	cd "$workdir"
	go test ./hat/hatCache -run '^TestSQLPlannerStatisticsPersistence' -count=1
	go test -race ./hat/hatCache -run '^TestSQLPlannerStatisticsPersistence' -count=1
	go vet ./hat/hatCache
)
