#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  hat/hatSql/typed_table.go \
  hat/hatSql/ch_u18_composite_primary_test.go \
  hat/hatSql/ch_u18_sparse_primary_baseline_benchmark_test.go \
  hat/hatSql/ch_u18_composite_primary_benchmark_test.go
