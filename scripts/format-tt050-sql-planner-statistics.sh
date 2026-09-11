#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/sql_planner_statistics.go hat/hatCache/sql_planner_statistics_test.go hat/hatCache/sql_planner_statistics_benchmark_test.go hat/hatSql/whatif.go hat/hatCache/sql_query.go hat/hatCache/main.go hat/hatCache/snapshot_restore_staged.go hat/hatCache/monitoring.go
