#!/usr/bin/env bash
set -euo pipefail

go test \
  hat/hatDataStructure/cursor_token.go \
  hat/hatDataStructure/cursor_token_encode_into_benchmark_test.go \
  -run '^$' \
  -bench '^BenchmarkCursorTokenEncodeIntoReuse$' \
  -benchmem \
  -count=10
