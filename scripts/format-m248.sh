#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/result_cache.go \
	hat/hatSql/m248_maintained_result_cache_test.go \
	hat/hatSql/m248_maintained_result_cache_benchmark_test.go
