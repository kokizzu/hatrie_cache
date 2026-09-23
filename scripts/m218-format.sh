#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/materialized.go hat/hatSql/query.go hat/hatSql/m218_materialized_view_point_planner_test.go hat/hatSql/m218_materialized_view_point_planner_benchmark_test.go
