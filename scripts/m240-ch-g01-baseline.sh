#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql \
  -run '^$' \
  -bench '^BenchmarkCH001HashJoinBaseline$' \
  -benchmem \
  -count=5
