#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/typed_table_column_ttl_snapshot.go \
  hat/hatSql/ch008_column_ttl_test.go
