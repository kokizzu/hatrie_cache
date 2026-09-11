#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query.go hat/hatSql/asof_join.go hat/hatSql/asof_join_test.go hat/hatSql/asof_join_benchmark_test.go hat/hatSql/asof_join_optimized_benchmark_test.go
