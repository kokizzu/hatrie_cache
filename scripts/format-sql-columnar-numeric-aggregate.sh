#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatCache/sql_columnar_scan_test.go hat/hatCache/sql_columnar_scan_benchmark_test.go
