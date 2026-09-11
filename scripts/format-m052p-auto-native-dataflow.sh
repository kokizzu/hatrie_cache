#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/query.go \
	 hat/hatSql/m052p_auto_native_dataflow.go \
	hat/hatSql/m052p_auto_native_dataflow_test.go \
	hat/hatSql/m052p_auto_native_dataflow_benchmark_test.go
