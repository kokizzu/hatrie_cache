#!/bin/sh
set -eu

gofmt -w \
hat/hatSql/enum_row_binary_benchmark_test.go \
hat/hatSql/enum_row_binary_test.go \
hat/hatSql/row_binary.go \
hat/hatSql/row_binary_enum.go \
hat/hatSql/row_binary_nullable_bitmap.go \
hat/hatSql/row_binary_read_stats.go \
hat/hatSql/row_binary_stats_pruning.go \
hat/hatSql/row_binary_stats.go \
hat/hatSql/row_binary_stream.go \
hat/hatSql/row_binary_delta_codec.go \
hat/hatSchema/compatibility.go \
hat/hatSchema/enum_test.go \
hat/hatSchema/fingerprint.go \
hat/hatSchema/generate.go
