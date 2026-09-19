#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatStorage/compaction_scheduler.go \
  hat/hatStorage/compaction_scheduler_io.go \
  hat/hatStorage/compaction_scheduler_stats.go \
  hat/hatStorage/chu28_io_throttle_test.go \
  hat/hatStorage/chu28_io_throttle_benchmark_test.go
