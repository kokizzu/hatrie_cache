#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/mz002_typed_table_compact_benchmark_test.go \
	hat/hatSql/mz002_typed_table_read_hold_benchmark_test.go \
	hat/hatSql/mz002_typed_table_read_hold_test.go \
	hat/hatSql/typed_table.go \
	hat/hatSql/typed_table_change_read_hold.go
