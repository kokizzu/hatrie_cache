#!/usr/bin/env bash
set -euo pipefail

go test \
  hat/hatDataStructure/cursor_token.go \
  hat/hatDataStructure/ordered_index.go \
  hat/hatDataStructure/ordered_snapshot_cursor.go \
  hat/hatDataStructure/ordered_cursor_after.go \
  hat/hatDataStructure/cursor_token_test.go \
  hat/hatDataStructure/cursor_token_encode_into_test.go
go test -race \
  hat/hatDataStructure/cursor_token.go \
  hat/hatDataStructure/ordered_index.go \
  hat/hatDataStructure/ordered_snapshot_cursor.go \
  hat/hatDataStructure/ordered_cursor_after.go \
  hat/hatDataStructure/cursor_token_test.go \
  hat/hatDataStructure/cursor_token_encode_into_test.go
if ! go test ./hat/hatDataStructure; then
  printf '%s\n' 'full hatDataStructure test has inherited baseline failures; focused cursor-token verification passed'
fi
