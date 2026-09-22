#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql \
  -run '^$' \
  -bench '^BenchmarkC230' \
  -benchmem \
  -count=5
