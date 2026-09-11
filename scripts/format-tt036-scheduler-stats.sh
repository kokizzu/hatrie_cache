#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatStorage/compaction_scheduler.go \
  hat/hatStorage/compaction_scheduler_stats.go \
  hat/hatStorage/compaction_scheduler_stats_benchmark_test.go \
  hat/hatStorage/compaction_scheduler_stats_test.go
