#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m052f_native_distinct_test.go \
  hat/hatSql/m052n_native_composite_distinct_test.go \
  hat/hatSql/m052n_native_composite_distinct_benchmark_test.go
