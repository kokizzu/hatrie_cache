#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/ch050_row_binary_import_benchmark_test.go \
  hat/hatCache/ch050_row_binary_import_test.go \
  hat/hatCache/sql_row_binary_import.go \
  hat/hatCache/sql_row_binary_http.go \
  hat/hatCache/monitoring.go \
  hat/hatSql/row_binary_stream.go
