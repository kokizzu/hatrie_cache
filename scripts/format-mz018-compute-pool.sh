#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query_manager.go hat/hatSql/mz018_compute_pool_test.go hat/hatSql/mz018_compute_pool_benchmark_test.go hat/hatCache/sql_query.go
