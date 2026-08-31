#!/bin/sh
set -eu

gofmt -w hat/hatSql/typed_table_arrangements.go hat/hatSql/typed_table_arrangements_test.go hat/hatSql/typed_table_arrangements_benchmark_test.go
