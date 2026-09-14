#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/tr018_zero_copy_row_binary.go \
  hat/hatSql/tr018_zero_copy_row_binary_test.go \
  hat/hatSql/tr018_zero_copy_row_binary_benchmark_test.go \
  hat/hatSql/tr018_zero_copy_row_binary_borrowed_benchmark_test.go
