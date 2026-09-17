#!/bin/sh
set -eu

gofmt -w hat/hatSql/ch036_aggregate_or_null.go hat/hatSql/ch036_aggregate_or_null_test.go hat/hatSql/ch036_aggregate_or_null_benchmark_test.go hat/hatSql/query.go
