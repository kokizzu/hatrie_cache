#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/incremental_range_boundary_window.go \
  hat/hatSql/m065q_incremental_range_boundary_benchmark_test.go \
  hat/hatSql/m065q_incremental_range_boundary_test.go
