#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/result_cache.go hat/hatSql/result_cache_test.go hat/hatSql/result_cache_benchmark_test.go
