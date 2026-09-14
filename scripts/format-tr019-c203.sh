#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/columnar_field_offsets.go \
  hat/hatSql/contracts.go \
  hat/hatSql/typed_table.go \
  hat/hatSql/tr019_tuple_field_offset_test.go \
  hat/hatSql/tr019_tuple_field_offset_benchmark_test.go
