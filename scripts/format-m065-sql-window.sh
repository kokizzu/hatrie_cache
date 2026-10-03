#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/m065_sql_incremental_window.go hat/hatSql/m065_sql_incremental_window_test.go hat/hatSql/query.go
