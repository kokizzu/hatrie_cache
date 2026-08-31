#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_columnar_sorted_projection_benchmark_test.go hat/hatCache/sql_columnar_sorted_projection_test.go hat/hatCache/sql_query.go hat/hatSql/contracts.go hat/hatSql/query.go
