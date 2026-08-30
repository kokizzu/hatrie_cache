#!/usr/bin/env sh
set -eu

gofmt -w hat/hatCache/main.go hat/hatCache/sql_query.go hat/hatCache/sql_typed_index_test.go hat/hatCache/sql_typed_index_benchmark_test.go
