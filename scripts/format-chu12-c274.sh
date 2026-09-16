#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatSql/index_rebuild_queue.go \
  hat/hatSql/ch_u12_index_rebuild_queue_test.go \
  hat/hatSql/ch_u12_index_rebuild_queue_baseline_benchmark_test.go \
  hat/hatSql/ch_u12_index_rebuild_queue_benchmark_test.go
