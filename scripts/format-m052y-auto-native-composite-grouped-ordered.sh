#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m052c_native_dataflow.go \
  hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/m052y_auto_native_composite_grouped_ordered_test.go \
  hat/hatSql/m052y_auto_native_composite_grouped_ordered_benchmark_test.go
