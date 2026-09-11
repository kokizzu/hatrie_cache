#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/row_binary.go \
  hat/hatSql/row_binary_parallel_test.go \
  hat/hatSql/row_binary_parallel_benchmark_test.go
