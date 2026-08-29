#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatCache/sql_columnar_numeric_aggregate_conjunction_test.go hat/hatCache/sql_columnar_numeric_aggregate_conjunction_benchmark_test.go
