#!/usr/bin/env sh
set -eu

gofmt -w hat/hatCache/sql_query.go hat/hatCache/sql_index_key_test.go hat/hatCache/sql_index_key_benchmark_test.go
