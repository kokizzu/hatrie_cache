#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/typed_table.go \
  hat/hatSql/typed_table_ttl.go \
  hat/hatSql/typed_table_stats.go \
  hat/hatSql/typed_table_histogram.go \
  hat/hatSql/ch008_column_ttl_test.go \
  hat/hatSql/ch008_column_ttl_benchmark_test.go
