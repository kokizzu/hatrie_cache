#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
  -run '^$' \
  -bench '^BenchmarkCursorTokenOperations$' \
  -benchmem \
  -count=5
