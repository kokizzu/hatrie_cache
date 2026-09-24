#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/mz038_sorted_arrangement_range.go \
  hat/hatSql/mz038_sorted_arrangement_range_test.go \
  hat/hatSql/mz038_sorted_arrangement_range_benchmark_test.go
