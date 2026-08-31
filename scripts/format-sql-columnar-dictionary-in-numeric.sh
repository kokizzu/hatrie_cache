#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql_columnar_dictionary_in_numeric_benchmark_test.go hat/hatCache/sql_columnar_dictionary_in_numeric_test.go hat/hatSql/query.go
