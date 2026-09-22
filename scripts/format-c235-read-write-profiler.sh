#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatSql/c235_read_write_profiler.go \
  hat/hatSql/c235_read_write_profiler_test.go \
  hat/hatSql/c235_read_write_profiler_baseline_benchmark_test.go
