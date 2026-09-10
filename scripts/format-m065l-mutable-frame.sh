#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/incremental_frame_window.go \
  hat/hatSql/mutable_frame_window.go \
  hat/hatSql/m065l_mutable_frame_window_example_test.go \
  hat/hatSql/m065l_mutable_frame_window_benchmark_test.go \
  hat/hatSql/m065l_mutable_frame_window_test.go
