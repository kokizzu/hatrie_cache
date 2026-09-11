#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/m052r_auto_native_ordered_test.go \
  hat/hatSql/m052r_auto_native_ordered_benchmark_test.go
