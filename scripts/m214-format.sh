#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatSql/typed_table_sorted_arrangements.go \
  hat/hatSql/m214_sorted_arrangements_test.go \
  hat/hatSql/m214_sorted_arrangements_benchmark_test.go
