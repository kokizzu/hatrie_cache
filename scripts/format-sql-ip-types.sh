#!/usr/bin/env bash
set -eu

gofmt -w \
  hat/hatSql/ip_types.go \
  hat/hatSql/ip_types_test.go \
  hat/hatSql/ip_row_binary_baseline_test.go \
  hat/hatCache/ip_sql_test.go \
  hat/hatCache/sql_query.go \
  hat/hatCache/sql.go \
  hat/hatSchema/ip_types_test.go \
  hat/hatSchema/schema.go \
  hat/hatSchema/generate.go \
  hat/hatSql/query.go \
  hat/hatSql/row_binary.go \
  hat/hatSql/row_binary_nullable_bitmap.go \
  hat/hatSql/row_binary_read_stats.go \
  hat/hatSql/row_binary_stats.go \
  hat/hatSql/row_binary_stream.go
