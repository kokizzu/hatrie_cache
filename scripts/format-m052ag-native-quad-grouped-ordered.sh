#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/m052ag_native_quad_grouped_ordered.go \
	hat/hatSql/m052ag_native_quad_grouped_ordered_test.go \
	hat/hatSql/m052ag_native_quad_grouped_ordered_benchmark_test.go \
	hat/hatSql/m052c_native_dataflow.go \
	hat/hatSql/m052p_auto_native_dataflow.go
