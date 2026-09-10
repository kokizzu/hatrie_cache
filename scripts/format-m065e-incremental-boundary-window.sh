#!/usr/bin/env bash
set -eu

	gofmt -w \
	hat/hatSql/incremental_boundary_window.go \
	hat/hatSql/m065e_incremental_boundary_window_test.go \
	hat/hatSql/m065e_incremental_boundary_window_benchmark_test.go \
	hat/hatSql/m065e_incremental_boundary_window_example_test.go
