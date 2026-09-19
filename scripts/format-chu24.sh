#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/typed_table.go \
  hat/hatSql/typed_table_columnar_append.go \
  hat/hatSql/typed_table_patch_parts.go \
  hat/hatSql/chu24_typed_table_memory_budget_test.go \
  hat/hatSql/chu24_typed_table_memory_budget_benchmark_test.go
