#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCodec/run_length_uint64.go \
  hat/hatCodec/run_length_uint64_test.go \
  hat/hatCodec/run_length_uint64_benchmark_test.go
