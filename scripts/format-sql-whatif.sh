#!/bin/sh
set -eu

gofmt -w hat/hatSql/whatif.go hat/hatSql/whatif_test.go hat/hatSql/whatif_benchmark_test.go hat/hatCache/sql_query.go hat/hatCache/sql_whatif_test.go
