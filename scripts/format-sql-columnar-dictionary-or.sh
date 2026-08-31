#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql_columnar_dictionary_or_benchmark_test.go hat/hatCache/sql_columnar_dictionary_or_test.go hat/hatSql/query.go
