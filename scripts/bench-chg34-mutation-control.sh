#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql \
  -run '^$' \
  -bench 'BenchmarkMutation' \
  -benchmem \
  -benchtime=100ms \
  -count=5
