#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/query_profiler.go \
  hat/hatSql/ch235_part_column_profiler.go \
  hat/hatSql/ch235_part_column_profiler_test.go \
  hat/hatSql/ch235_part_column_profiler_benchmark_test.go
