#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication \
  -run '^$' \
  -bench 'BenchmarkC249' \
  -benchmem \
  -count=5
