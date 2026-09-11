#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/incremental_range_window.go \
  hat/hatSql/m065o_incremental_range_distinct_benchmark_test.go \
  hat/hatSql/m065o_incremental_range_distinct_test.go
