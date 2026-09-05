#!/bin/sh
set -eu

gofmt -w \
	hat/hatSql/contracts.go \
	hat/hatSql/query.go \
	hat/hatSql/typed_table.go \
	hat/hatSql/sparse_primary_index_test.go \
	hat/hatSql/typed_table_sparse_primary_test.go \
	hat/hatSql/columnar_segment_skip_test.go \
	hat/hatSql/sparse_primary_index_benchmark_test.go
