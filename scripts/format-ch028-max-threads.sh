#!/bin/sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatSql/ch028_max_threads_test.go hat/hatSql/ch028_max_threads_benchmark_test.go hat/hatCache/sql_query.go
