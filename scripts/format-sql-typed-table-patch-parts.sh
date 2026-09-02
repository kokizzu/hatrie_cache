#!/bin/sh
set -eu

gofmt -w \
  hat/hatSql/typed_table.go \
  hat/hatSql/typed_table_patch_parts.go \
  hat/hatSql/typed_table_patch_parts_test.go \
  hat/hatSql/typed_table_patch_parts_benchmark_test.go
