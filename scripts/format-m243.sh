#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/typed_table_arrangement_stats.go \
  hat/hatSql/m243_arrangement_memory_benchmark_test.go \
  hat/hatSql/m243_arrangement_memory_test.go
