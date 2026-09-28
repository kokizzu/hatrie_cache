#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m038_sql_incremental_distinct.go \
  hat/hatSql/m038_sql_incremental_distinct_test.go \
  hat/hatSql/m038_sql_incremental_distinct_benchmark_test.go \
  hat/hatSql/mz034_sql_incremental_projection.go
