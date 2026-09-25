#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatStorage/remote_part_cache.go \
  hat/hatStorage/tt015_read_worker_tuning_test.go \
  hat/hatStorage/tt015_read_worker_tuning_benchmark_test.go
