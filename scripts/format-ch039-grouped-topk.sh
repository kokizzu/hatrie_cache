#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/approx_aggregate.go \
  hat/hatSql/m052c_native_dataflow.go \
  hat/hatSql/ch039_grouped_topk_native_test.go \
  hat/hatSql/ch039_grouped_topk_benchmark_test.go
