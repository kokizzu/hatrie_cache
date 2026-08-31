#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql_columnar_layout_cache.go hat/hatCache/sql_query.go hat/hatCache/sql_columnar_topn_layout_preference_benchmark_test.go hat/hatCache/sql_columnar_topn_layout_preference_test.go hat/hatSql/contracts.go hat/hatSql/query.go
