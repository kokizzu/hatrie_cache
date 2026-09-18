#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql \
  -run '^$' \
  -bench '^BenchmarkCH010' \
  -benchmem \
  -benchtime=100000x \
  -count=5 \
  -v
