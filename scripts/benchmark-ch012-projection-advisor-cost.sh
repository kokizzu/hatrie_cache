#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql \
  -run '^$' \
  -bench '^BenchmarkCH012ProjectionAdvisor' \
  -benchmem \
  -benchtime=1000x \
  -count=5 \
  -v
