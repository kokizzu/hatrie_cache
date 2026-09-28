#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCodec/float64_xor.go \
  hat/hatCodec/float64_xor_test.go \
  hat/hatCodec/float64_xor_benchmark_test.go
