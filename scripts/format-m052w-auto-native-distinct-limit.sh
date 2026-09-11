#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m052c_native_dataflow.go \
  hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/m052w_auto_native_distinct_limit_test.go \
  hat/hatSql/m052w_auto_native_distinct_limit_benchmark_test.go
