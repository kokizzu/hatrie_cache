#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/incremental_range_window.go \
  hat/hatSql/m065m_incremental_range_window_benchmark_test.go \
  hat/hatSql/m065n_incremental_range_window_test.go
