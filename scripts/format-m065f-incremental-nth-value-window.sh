#!/usr/bin/env bash
set -eu

	gofmt -w \
	hat/hatSql/incremental_nth_value_window.go \
	hat/hatSql/m065f_incremental_nth_value_window_test.go \
	hat/hatSql/m065f_incremental_nth_value_window_benchmark_test.go \
	hat/hatSql/m065f_incremental_nth_value_window_example_test.go
