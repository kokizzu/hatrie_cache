#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql \
  -run '^$' \
  -bench '^BenchmarkM065LFrameMutation/(full_recompute|mutable_update)$' \
  -benchmem \
  -benchtime=200ms \
  -count=5
