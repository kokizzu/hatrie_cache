#!/bin/sh
set -eu

gofmt -w hat/hatSql/c206_query_cache_eligibility_test.go hat/hatSql/c206_query_cache_eligibility_benchmark_test.go
