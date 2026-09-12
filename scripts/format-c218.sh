#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/with_fill.go \
  hat/hatSql/with_fill_query.go \
  hat/hatSql/c218_with_fill_interpolation.go \
  hat/hatSql/c218_with_fill_interpolation_test.go
