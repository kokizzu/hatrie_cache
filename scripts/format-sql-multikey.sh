#!/bin/sh
set -eu

gofmt -w hat/hatSql/query.go

gofmt -w \
  api.go \
  hat/hatCache/sql_borrowed_index.go \
  hat/hatCache/sql_index_consistency.go \
  hat/hatCache/sql_multikey_index.go \
  hat/hatCache/sql_multikey_index_test.go \
  hat/hatCache/sql_multikey_index_benchmark_test.go \
  hat/hatCache/sql_query.go \
  hat/hatSql/contracts.go
