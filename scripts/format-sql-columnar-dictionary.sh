#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/contracts.go hat/hatSql/query.go hat/hatCache/sql_query.go hat/hatCache/sql_columnar_scan_test.go hat/hatCache/sql_columnar_scan_benchmark_test.go
