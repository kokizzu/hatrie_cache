#!/usr/bin/env bash
set -euo pipefail
	gofmt -w \
	 hat/hatSql/m065x_mutable_range_boundary_window.go \
	 hat/hatSql/m065x_mutable_range_boundary_window_test.go \
	 hat/hatSql/m065x_mutable_range_boundary_window_benchmark_test.go
