#!/usr/bin/env bash
set -euo pipefail

go test \
  hat/hatDataStructure/cursor_token.go \
  hat/hatDataStructure/cursor_token_benchmark_test.go \
  -run '^$' \
  -bench '^BenchmarkCursorTokenOperations$' \
  -benchmem \
  -count=10
