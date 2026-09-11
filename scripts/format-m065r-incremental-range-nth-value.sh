#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/incremental_range_nth_value_window.go \
  hat/hatSql/m065r_incremental_range_nth_value_benchmark_test.go \
  hat/hatSql/m065r_incremental_range_nth_value_test.go
