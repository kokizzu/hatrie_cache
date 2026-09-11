#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m052p_auto_native_dataflow.go \
  hat/hatSql/m052q_auto_native_operators_test.go \
  hat/hatSql/m052q_auto_native_operators_benchmark_test.go
