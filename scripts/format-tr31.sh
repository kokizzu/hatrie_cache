#!/bin/sh
set -eu

gofmt -w hat/hatCache/tr031_index_selection_benchmark_test.go hat/hatCache/sql_adaptive_planner_test.go hat/hatSql/query.go
