#!/bin/sh
set -eu

gofmt -w \
	hat/hatSql/result_cache.go \
	hat/hatSql/sql_result_cache.go \
	hat/hatSql/query.go \
	hat/hatSql/result_cache_persistence.go \
	hat/hatSql/chu40_dependency_invalidation_test.go \
	hat/hatSql/chu40_dependency_invalidation_benchmark_test.go
