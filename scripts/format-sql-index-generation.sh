#!/bin/sh
set -eu

gofmt -w hat/hatCache/main.go hat/hatCache/sql_query.go hat/hatCache/sql_index_snapshot_test.go hat/hatCache/sql_index_generation_benchmark_test.go
