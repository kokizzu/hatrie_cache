#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
gofmt -w \
    "$repo_root/hat/hatSql/row_binary_nullable_bitmap.go" \
    "$repo_root/hat/hatSql/row_binary_bitmap_benchmark_test.go" \
    "$repo_root/hat/hatSql/row_binary_bitmap_into_test.go" \
    "$repo_root/hat/hatSql/row_binary_bitmap_into_benchmark_test.go"
