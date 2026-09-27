#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m052ah_auto_native_count_distinct_benchmark_test.go \
  hat/hatSql/m052ah_auto_native_count_distinct_test.go \
  hat/hatSql/m052c_native_dataflow.go \
  hat/hatSql/query.go
