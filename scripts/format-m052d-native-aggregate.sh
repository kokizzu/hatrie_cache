#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m052c_native_dataflow.go \
  hat/hatSql/m052c_native_dataflow_test.go \
  hat/hatSql/m052d_native_aggregate_benchmark_test.go \
  hat/hatSql/m052d_native_aggregate_test.go
