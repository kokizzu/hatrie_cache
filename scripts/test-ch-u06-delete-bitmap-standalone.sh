#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql/columnar_delete_bitmap.go ./hat/hatSql/ch_u06_delete_bitmap_test.go -run '^TestSQLColumnarDeleteBitmap' -count=1 "$@"
