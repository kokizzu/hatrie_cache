#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/ch039_grouped_approx_native_test.go \
  hat/hatSql/ch039_grouped_approx_benchmark_test.go \
  hat/hatSql/m052c_native_dataflow.go \
  hat/hatSql/m052ae_native_triple_group.go \
  hat/hatSql/m052ag_native_quad_grouped_ordered.go
