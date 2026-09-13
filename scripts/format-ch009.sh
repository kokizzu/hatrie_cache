#!/usr/bin/env bash
set -eu

gofmt -w \
  hat/hatCache/ch009_async_insert_buffer.go \
  hat/hatCache/ch009_async_insert_buffer_test.go \
  hat/hatCache/ch009_async_insert_buffer_benchmark_test.go
