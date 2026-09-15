#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatCache/ch008_auto_result_cache_test.go \
	hat/hatCache/ch008_auto_result_cache_benchmark_test.go \
	hat/hatCache/main.go \
	hat/hatCache/sql_query.go \
	hat/hatCache/sql_result_cache_auto.go
