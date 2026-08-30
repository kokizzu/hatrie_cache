#!/bin/sh
set -eu

gofmt -w hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/columnar_source_borrowed_test.go hat/hatCache/sql_columnar_borrowed_test.go hat/hatCache/sql_columnar_borrowed_benchmark_test.go hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_query.go
