#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/maintenance_job_queue.go \
  hat/hatSql/index_rebuild_queue.go \
  hat/hatSql/ch_g32_maintenance_queue_test.go \
  hat/hatSql/ch_g32_maintenance_queue_benchmark_test.go
