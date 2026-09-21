#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/query.go \
  hat/hatSql/array_join_test.go \
  hat/hatSql/ch037_left_array_join_benchmark_test.go
