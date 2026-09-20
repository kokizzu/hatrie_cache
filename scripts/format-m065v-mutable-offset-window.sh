#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m065v_mutable_offset_window.go \
  hat/hatSql/m065v_mutable_offset_window_test.go \
  hat/hatSql/m065v_mutable_offset_window_baseline_benchmark_test.go
