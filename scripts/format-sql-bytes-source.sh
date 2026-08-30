#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql_query.go hat/hatCache/sql_bytes_source_test.go hat/hatCache/sql_bytes_source_benchmark_test.go
