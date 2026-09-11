#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/query.go \
  hat/hatSql/sql_set_operation_all_test.go \
  hat/hatSql/sql_set_operation_all_benchmark_test.go \
  hat/hatCache/sql_function_test.go
