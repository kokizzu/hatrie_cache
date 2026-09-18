#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/mu012_arrangement_explain.go \
  hat/hatSql/mz024_arrangement_selection.go \
  hat/hatSql/mz024_arrangement_selection_test.go \
  hat/hatSql/mz024_arrangement_selection_public_test.go \
  hat/hatSql/query.go
