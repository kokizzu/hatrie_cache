#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql_columnar_dictionary_like_benchmark_test.go hat/hatCache/sql_columnar_dictionary_like_test.go hat/hatSql/query.go
