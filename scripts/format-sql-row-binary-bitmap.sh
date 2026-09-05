#!/bin/sh
set -eu

gofmt -w hat/hatSql/row_binary_nullable_bitmap.go hat/hatSql/row_binary_nullable_bitmap_test.go
