#!/bin/sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatCache/sql_columnar_mixed_conjunction_test.go hat/hatCache/sql_columnar_mixed_conjunction_benchmark_test.go
