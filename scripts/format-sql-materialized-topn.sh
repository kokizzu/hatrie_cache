#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatCache/sql_production_test.go hat/hatCache/sql_materialized_topn_benchmark_test.go
