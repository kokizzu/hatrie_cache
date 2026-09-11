#!/usr/bin/env bash
set -euo pipefail

git add -- ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md README.md BENCHMARK.md SQL_PLANNER_STATISTICS.md Makefile hat/hatCache/main.go hat/hatCache/sql_query.go hat/hatCache/sql_planner_statistics.go hat/hatCache/sql_planner_statistics_test.go hat/hatCache/sql_planner_statistics_benchmark_test.go hat/hatCache/snapshot_restore_staged.go hat/hatCache/monitoring.go hat/hatSql/whatif.go scripts/test-tt050-sql-planner-statistics.sh scripts/format-tt050-sql-planner-statistics.sh scripts/benchmark-tt050-sql-planner-statistics.sh scripts/test-race-tt050-sql-planner-statistics.sh scripts/verify-tt050-sql-planner-statistics-docs.sh scripts/review-tt050-sql-planner-statistics.sh scripts/commit-tt050-sql-planner-statistics.sh scripts/push-tt050-sql-planner-statistics.sh
git diff --cached --check
git commit -m "sql: add source planner statistics"
