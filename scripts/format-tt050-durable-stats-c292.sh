#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/sql_planner_statistics_persistence.go hat/hatCache/sql_planner_statistics_test.go hat/hatCache/tt050_sql_planner_statistics_persistence_benchmark_test.go
