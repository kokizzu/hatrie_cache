#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff -- \
  hat/hatSql/compiled_ir.go \
  hat/hatSql/dataflow_executor.go \
  hat/hatSql/m052c_native_dataflow.go \
  hat/hatSql/m052c_native_dataflow_test.go \
  hat/hatSql/m052c_native_dataflow_benchmark_test.go \
  SQL_DATAFLOW_EXECUTOR.md \
  INSPIRATION.md
