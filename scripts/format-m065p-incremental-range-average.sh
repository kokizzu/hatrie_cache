#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/incremental_range_window.go \
  hat/hatSql/m065p_incremental_range_average_benchmark_test.go \
  hat/hatSql/m065p_incremental_range_average_test.go
