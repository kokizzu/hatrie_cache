#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/tt049_row_locks.go \
  hat/hatSql/tt049_row_locks_test.go \
  hat/hatSql/tt049_row_locks_public_test.go
