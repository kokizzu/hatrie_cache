#!/bin/sh
set -eu

gofmt -w \
hat/hatSchema/compatibility.go \
hat/hatSchema/decimal_test.go \
hat/hatSchema/fingerprint.go \
hat/hatSchema/generate.go \
hat/hatSchema/schema.go \
hat/hatSql/decimal_row_binary_benchmark_test.go \
hat/hatSql/decimal_types.go \
hat/hatSql/decimal_types_test.go \
hat/hatSql/query.go \
hat/hatSql/row_binary.go \
hat/hatSql/row_binary_delta_codec.go \
hat/hatSql/row_binary_nullable_bitmap.go \
hat/hatSql/row_binary_read_stats.go \
hat/hatSql/row_binary_stats.go \
hat/hatSql/row_binary_stream.go
gofmt -w hat/hatSql/row_binary_dictionary.go
