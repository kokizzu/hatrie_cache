#!/bin/sh
set -eu

gofmt -w hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/columnar_segment_skip_test.go hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_query.go hat/hatCache/sql_columnar_borrowed_benchmark_test.go hat/hatCache/sql_columnar_segment_skip_test.go hat/hatCache/sql_columnar_segment_skip_benchmark_test.go
