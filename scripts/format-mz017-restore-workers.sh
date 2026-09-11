#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/local_partition.go \
  hat/hatCache/main.go \
  hat/hatCache/snapshot_restore_staged.go \
  hat/hatCache/snapshot_restore_workers_test.go \
  hat/hatCache/local_partition_restore_benchmark_test.go
