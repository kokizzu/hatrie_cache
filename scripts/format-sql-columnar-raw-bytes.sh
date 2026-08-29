#!/usr/bin/env sh
set -eu

gofmt -w hat/hatCache/sql_query.go hat/hatCache/sql_columnar_raw_bytes_test.go hat/hatCache/sql_columnar_raw_bytes_benchmark_test.go
