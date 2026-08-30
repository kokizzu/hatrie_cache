#!/bin/sh
set -eu

gofmt -w hat/hatSql/condition_cache.go hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/query_condition_cache_benchmark_test.go hat/hatSql/query_condition_cache_test.go
