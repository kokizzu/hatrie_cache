#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql_columnar_dictionary_group_unordered_benchmark_test.go hat/hatCache/sql_columnar_dictionary_group_unordered_test.go hat/hatSql/query.go
