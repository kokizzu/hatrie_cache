#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  hat/hatCache/sql_query.go \
  hat/hatCache/ch058_bitmap_in_test.go \
  hat/hatCache/ch058_bitmap_in_benchmark_test.go
