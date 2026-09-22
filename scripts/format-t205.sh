#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/t205_replication_progress_metrics.go \
  hat/hatReplication/t205_replication_progress_metrics_test.go \
  hat/hatReplication/t205_replication_progress_metrics_benchmark_test.go \
  hat/hatReplication/t205_replication_progress_metrics_baseline_benchmark_test.go
