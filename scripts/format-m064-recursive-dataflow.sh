#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatSql/m064_recursive_dataflow.go \
  hat/hatSql/m064_recursive_dataflow_test.go \
  hat/hatSql/m064_recursive_dataflow_baseline_benchmark_test.go \
  hat/hatSql/m064_recursive_dataflow_benchmark_test.go
