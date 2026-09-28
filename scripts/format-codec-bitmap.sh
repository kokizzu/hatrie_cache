#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCodec/bool_bitmap.go \
  hat/hatCodec/bool_bitmap_test.go \
  hat/hatCodec/bool_bitmap_benchmark_test.go
