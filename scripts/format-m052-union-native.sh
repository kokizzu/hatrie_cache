#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
    hat/hatSql/m052p_auto_native_dataflow.go \
    hat/hatSql/m052p_auto_native_dataflow_test.go \
    hat/hatSql/m052z_auto_native_union_benchmark_test.go \
    hat/hatSql/m052z_native_union.go \
    hat/hatSql/query.go
