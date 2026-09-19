#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatStorage/remote_part_gc.go \
  hat/hatStorage/chu32_remote_part_gc_test.go \
  hat/hatStorage/chu32_remote_part_gc_benchmark_test.go
