#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql_columnar_dictionary_group_in_benchmark_test.go hat/hatCache/sql_columnar_dictionary_group_in_test.go hat/hatSql/query.go
