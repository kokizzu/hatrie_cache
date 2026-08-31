#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql_columnar_aggregate_dictionary_in_benchmark_test.go hat/hatCache/sql_columnar_aggregate_dictionary_in_test.go hat/hatSql/query.go
