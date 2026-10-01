#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/columnar_delete_bitmap.go hat/hatSql/ch_u06_delete_bitmap_test.go
