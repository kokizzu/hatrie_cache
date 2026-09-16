#!/bin/sh
set -eu

gofmt -w \
  hat/hatCache/async_command_http.go \
  hat/hatCache/async_command_http_test.go \
  hat/hatCache/async_command_http_benchmark_test.go \
  hat/hatCache/monitoring.go
