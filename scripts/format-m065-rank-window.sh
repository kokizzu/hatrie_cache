#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/incremental_rank_window.go \
  hat/hatSql/m065_rank_window_benchmark_test.go \
  hat/hatSql/m065_rank_window_test.go
