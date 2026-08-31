#!/bin/sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatSql/columnar_distinct_test.go hat/hatSql/columnar_distinct_benchmark_test.go
