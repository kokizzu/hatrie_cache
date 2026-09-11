#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/m052v_auto_native_scalar_limit_test.go \
  hat/hatSql/m052v_auto_native_scalar_limit_benchmark_test.go
