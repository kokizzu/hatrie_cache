#!/usr/bin/env sh
set -eu

gofmt -w hat/hatCache/sql_query.go hat/hatCache/sql_index_snapshot_test.go hat/hatCache/sql_index_snapshot_benchmark_test.go
