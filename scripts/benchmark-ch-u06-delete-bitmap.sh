#!/usr/bin/env bash
set -euo pipefail

count="${BENCH_COUNT:-5}"
time_per_benchmark="${BENCH_TIME:-200ms}"
go test ./hat/hatSql/columnar_delete_bitmap.go ./hat/hatSql/ch_u06_delete_bitmap_test.go ./hat/hatSql/ch_u06_delete_bitmap_benchmark_test.go -run '^$' -bench '^BenchmarkSQLColumnarDeleteBitmap' -benchmem -count="$count" -benchtime="$time_per_benchmark" "$@"
