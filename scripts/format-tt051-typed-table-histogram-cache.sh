#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/typed_table.go \
  hat/hatSql/typed_table_histogram.go \
  hat/hatSql/c210_typed_table_histogram_test.go
