#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m037_incremental_group_min_max.go \
  hat/hatSql/m037_sql_incremental_group_min_max_test.go \
  hat/hatSql/m037_sql_incremental_group_min_max_benchmark_test.go \
  hat/hatSql/mz034_sql_incremental_group.go
