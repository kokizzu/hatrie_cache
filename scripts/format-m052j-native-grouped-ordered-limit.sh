#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m052c_native_dataflow.go \
  hat/hatSql/m052j_native_grouped_ordered_limit_test.go \
  hat/hatSql/m052j_native_grouped_ordered_limit_benchmark_test.go
