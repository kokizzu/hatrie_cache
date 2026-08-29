#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatSql/prepared_cache_test.go hat/hatSql/prepared_cache_benchmark_test.go
