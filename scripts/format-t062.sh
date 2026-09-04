#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/monitoring.go \
  hat/hatCache/replication.go \
  hat/hatCache/replication_bandwidth.go \
  hat/hatCache/replication_bandwidth_test.go \
  hat/hatReplication/metrics.go \
  hat/hatReplication/metrics_test.go \
  hat/hatReplication/metrics_benchmark_test.go
