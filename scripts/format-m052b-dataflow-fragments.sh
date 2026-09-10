#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/dataflow_executor.go \
  hat/hatSql/compiled_ir.go \
  hat/hatSql/m052b_dataflow_fragment_test.go \
  hat/hatSql/m052b_dataflow_fragment_benchmark_test.go \
  hat/hatSql/m052b_dataflow_fragment_example_test.go
