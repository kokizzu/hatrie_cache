#!/usr/bin/env bash
set -eu

gofmt -w hat/hatSql/incremental_rank_window.go hat/hatSql/mutable_rank_window.go hat/hatSql/m065_mutable_rank_window_test.go hat/hatSql/m065_rank_window_benchmark_test.go
