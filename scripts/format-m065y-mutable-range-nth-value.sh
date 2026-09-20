#!/usr/bin/env bash
set -euo pipefail
	gofmt -w \
	 hat/hatSql/m065y_mutable_range_nth_value_window.go \
	 hat/hatSql/m065y_mutable_range_nth_value_window_test.go \
	 hat/hatSql/m065y_mutable_range_nth_value_window_benchmark_test.go
