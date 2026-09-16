#!/bin/sh
set -eu

gofmt -w \
	hat/hatSql/query.go \
	hat/hatSql/sql_result_cache.go \
	hat/hatSql/c205_subquery_result_cache_test.go \
	hat/hatSql/c205_subquery_result_cache_benchmark_test.go
