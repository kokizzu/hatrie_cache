#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/typed_table_arrangements.go \
  hat/hatSql/typed_table_join_arrangements.go \
  hat/hatSql/typed_table_arrangement_hydration_test.go \
  hat/hatSql/typed_table_arrangement_hydration_benchmark_test.go
