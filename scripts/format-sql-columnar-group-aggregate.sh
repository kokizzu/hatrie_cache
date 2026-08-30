#!/bin/sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatSql/columnar_group_aggregate_test.go hat/hatSql/columnar_group_aggregate_benchmark_test.go
