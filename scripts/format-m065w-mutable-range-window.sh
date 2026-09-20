#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m065w_mutable_range_window.go \
  hat/hatSql/m065w_mutable_range_window_test.go \
  hat/hatSql/m065w_mutable_range_window_baseline_benchmark_test.go
