#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatSql/mz034_sql_incremental_group.go \
  hat/hatSql/mz034_sql_incremental_group_test.go \
  hat/hatSql/mz034_sql_incremental_group_baseline_test.go
