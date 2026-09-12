#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql \
  -run '^$' \
  -bench '^BenchmarkC218FillInterpolation(Baseline|Linear)$' \
  -benchmem \
  -count=5 \
  -cpu=1
