#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/typed_table_arrangement_stats.go \
  hat/hatSql/typed_table_arrangement_stats_benchmark_test.go \
  hat/hatSql/typed_table_arrangement_stats_test.go
