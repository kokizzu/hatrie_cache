#!/usr/bin/env bash
set -eu

	gofmt -w \
	hat/hatSql/incremental_frame_window.go \
	hat/hatSql/m065g_incremental_extrema_frame_window_test.go \
	hat/hatSql/m065g_incremental_extrema_frame_window_benchmark_test.go \
	hat/hatSql/m065g_incremental_extrema_frame_window_example_test.go
