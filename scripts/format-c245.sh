#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatStorage/remote_part_cache.go \
  hat/hatStorage/remote_part_cache_c245_test.go \
  hat/hatStorage/remote_part_cache_c245_benchmark_test.go
