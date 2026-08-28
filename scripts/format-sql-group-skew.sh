#!/bin/sh
set -eu

gofmt -w ./hat/hatSql/query.go ./hat/hatCache/sql_group_skew_test.go ./hat/hatCache/sql_group_skew_benchmark_test.go
