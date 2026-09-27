#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/ch016_async_insert_sql.go hat/hatCache/ch016_async_insert_sql_test.go hat/hatCache/ch016_async_insert_sql_benchmark_test.go
