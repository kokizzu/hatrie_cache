#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m052c_native_dataflow.go \
  hat/hatSql/m052k_native_grouped_having_test.go \
  hat/hatSql/m052k_native_grouped_having_benchmark_test.go
