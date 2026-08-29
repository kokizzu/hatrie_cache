#!/bin/sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatSql/regex.go hat/hatCache/sql_columnar_regexp_test.go hat/hatCache/sql_columnar_regexp_benchmark_test.go
