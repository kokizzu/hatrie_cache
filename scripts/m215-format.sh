#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatSql/typed_table_join.go \
  hat/hatSql/typed_table_join_arrangements.go \
  hat/hatSql/typed_table_join_deltas.go \
  hat/hatSql/m215_join_deltas_test.go \
  hat/hatSql/m215_join_deltas_benchmark_test.go
