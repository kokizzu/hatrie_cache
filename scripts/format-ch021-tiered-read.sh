#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatStorage/ch021_tiered_reader.go \
  hat/hatStorage/ch021_tiered_read_test.go \
  hat/hatStorage/ch021_tiered_read_baseline_benchmark_test.go
