#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCodec/int64_delta.go \
  hat/hatCodec/int64_delta_test.go \
  hat/hatCodec/int64_delta_benchmark_test.go
