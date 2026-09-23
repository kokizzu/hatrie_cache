#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md INSPIRATION_ROUND2.md M218_MATERIALIZED_POINT_PLANNER.md hat/hatSql/materialized.go hat/hatSql/query.go hat/hatSql/m218_materialized_view_point_planner_test.go hat/hatSql/m218_materialized_view_point_planner_benchmark_test.go scripts/m218-test.sh scripts/m218-benchmark.sh scripts/m218-format.sh scripts/m218-race.sh scripts/m218-vet.sh scripts/m218-stage.sh scripts/m218-commit.sh scripts/m218-push.sh
git diff --cached --check
git diff --cached --stat
git status --short
