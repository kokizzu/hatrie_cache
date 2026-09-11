#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m052c_native_dataflow.go \
  hat/hatSql/query.go \
  hat/hatSql/m052e_native_group_benchmark_test.go \
  hat/hatSql/m052e_native_group_test.go
