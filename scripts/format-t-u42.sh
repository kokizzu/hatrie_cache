#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/cursor_token.go \
  hat/hatDataStructure/ordered_cursor_after.go \
  hat/hatDataStructure/cursor_token_test.go \
  hat/hatDataStructure/cursor_token_benchmark_test.go
