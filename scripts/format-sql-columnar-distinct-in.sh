#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql_columnar_distinct_in_benchmark_test.go hat/hatCache/sql_columnar_distinct_in_test.go hat/hatSql/query.go
