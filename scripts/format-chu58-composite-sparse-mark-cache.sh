#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/ch_u58_composite_sparse_mark_cache_test.go hat/hatSql/ch_u58_composite_sparse_mark_cache_benchmark_test.go hat/hatSql/typed_table_sparse_mark_cache.go
