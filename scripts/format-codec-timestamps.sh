#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCodec/compact_timestamp.go \
  hat/hatCodec/compact_timestamp_test.go \
  hat/hatCodec/compact_timestamp_benchmark_test.go
