#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatSql/columnar_limit_pushdown_test.go hat/hatSql/columnar_limit_pushdown_benchmark_test.go
