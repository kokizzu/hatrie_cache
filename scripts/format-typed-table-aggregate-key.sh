#!/bin/sh
set -eu

gofmt -w \
  hat/hatSql/typed_table.go \
  hat/hatSql/typed_table_aggregate_key.go \
  hat/hatSql/typed_table_aggregate_key_test.go
