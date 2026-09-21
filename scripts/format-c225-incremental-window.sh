#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/ch225_incremental_window.go \
  hat/hatSql/ch225_incremental_window_benchmark_test.go \
  hat/hatSql/ch225_incremental_window_test.go
