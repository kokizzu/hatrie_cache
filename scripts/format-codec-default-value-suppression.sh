#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCodec/default_value_suppression.go \
  hat/hatCodec/default_value_suppression_test.go \
  hat/hatCodec/default_value_suppression_benchmark_test.go
