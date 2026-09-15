#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/ch007_row_ttl_benchmark_test.go \
	hat/hatSql/ch225_ttl_expiry_index_test.go \
	hat/hatSql/ch225_ttl_memory_benchmark_test.go \
	hat/hatSql/typed_table.go \
	hat/hatSql/typed_table_patch_parts.go \
	hat/hatSql/typed_table_ttl.go \
	hat/hatSql/typed_table_ttl_expiry_index.go
