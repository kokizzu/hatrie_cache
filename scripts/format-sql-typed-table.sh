#!/bin/sh
set -eu

gofmt -w hat/hatSql/typed_table.go hat/hatSql/typed_table_test.go hat/hatSql/typed_table_benchmark_test.go
