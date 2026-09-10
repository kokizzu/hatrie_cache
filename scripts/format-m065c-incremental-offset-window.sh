#!/usr/bin/env bash
set -eu

gofmt -w hat/hatSql/incremental_offset_window.go hat/hatSql/m065c_incremental_offset_window_test.go hat/hatSql/m065c_incremental_offset_window_benchmark_test.go
