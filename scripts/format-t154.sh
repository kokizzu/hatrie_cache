#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/async_command_http.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/slow_command_capture.go \
  hat/hatCache/slow_command_capture_test.go \
  hat/hatCache/slow_command_capture_benchmark_test.go
