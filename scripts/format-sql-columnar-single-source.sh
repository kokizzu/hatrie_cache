#!/bin/sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatSql/sql_columnar_single_source_test.go hat/hatCache/sql_columnar_generic_filter_benchmark_test.go
