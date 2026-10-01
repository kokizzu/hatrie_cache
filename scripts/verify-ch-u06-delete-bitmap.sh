#!/usr/bin/env bash
set -euo pipefail

gofmt -d hat/hatSql/columnar_delete_bitmap.go hat/hatSql/ch_u06_delete_bitmap_test.go hat/hatSql/ch_u06_delete_bitmap_benchmark_test.go
go test ./hat/hatSql/columnar_delete_bitmap.go ./hat/hatSql/ch_u06_delete_bitmap_test.go -run '^TestSQLColumnarDeleteBitmap' -count=1
go test -race ./hat/hatSql/columnar_delete_bitmap.go ./hat/hatSql/ch_u06_delete_bitmap_test.go -run '^TestSQLColumnarDeleteBitmap' -count=1
go vet ./hat/hatSql/columnar_delete_bitmap.go ./hat/hatSql/ch_u06_delete_bitmap_test.go
git diff --check -- Makefile README.md PRODUCT_IDEA_GAPS.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md CHU06_PERSISTENT_DELETE_BITMAP.md hat/hatSql/columnar_delete_bitmap.go hat/hatSql/ch_u06_delete_bitmap_test.go hat/hatSql/ch_u06_delete_bitmap_benchmark_test.go scripts
