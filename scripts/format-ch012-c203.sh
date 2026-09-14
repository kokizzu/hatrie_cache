#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/ch012_delete_bitmap_test.go hat/hatSql/ch012_delete_bitmap_benchmark_test.go hat/hatSql/typed_table_delete_bitmap.go hat/hatSql/typed_table_patch_parts.go hat/hatSql/typed_table.go
