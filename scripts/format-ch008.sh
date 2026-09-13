#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
	hat/hatSql/query.go \
	hat/hatSql/sql_result_cache.go \
	hat/hatSql/c208_query_cache_metrics_test.go \
	hat/hatSql/c208_result_cache_key_benchmark_test.go
