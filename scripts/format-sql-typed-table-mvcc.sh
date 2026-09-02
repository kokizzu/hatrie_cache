#!/usr/bin/env bash
set -eu

gofmt -w \
  hat/hatSql/typed_table.go \
  hat/hatSql/typed_table_mvcc.go \
  hat/hatSql/typed_table_mvcc_test.go \
  hat/hatSql/typed_table_mvcc_benchmark_test.go
