#!/bin/sh
set -eu
gofmt -w hat/hatSql/mz030_lookup_join_cache.go hat/hatSql/mz030_lookup_cache_benchmark_test.go hat/hatSql/mz030_lookup_join_cache_test.go
