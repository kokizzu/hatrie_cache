#!/bin/sh
set -eu

gofmt -w hat/hatCache/main.go hat/hatCache/monitoring.go hat/hatCache/sql_query.go hat/hatCache/sql_typed_composite_test.go hat/hatCache/sql_typed_composite_benchmark_test.go hat/hatSql/contracts.go hat/hatSql/query.go
