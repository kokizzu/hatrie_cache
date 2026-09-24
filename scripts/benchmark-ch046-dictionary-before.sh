#!/usr/bin/env bash
set -euo pipefail

GOTOOLCHAIN=auto go test ./hat/hatSql -run '^$' \
  -bench '^BenchmarkCH046(Current|Legacy|Adaptive|HuffmanOnly)ColumnarStream$' \
  -benchmem -count=5
