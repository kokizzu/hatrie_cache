#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatStorage/compaction_scheduler_stats.go \
  hat/hatStorage/compaction_diagnostics.go \
  hat/hatStorage/c239_compaction_metrics_test.go \
  hat/hatStorage/c239_compaction_metrics_benchmark_test.go
