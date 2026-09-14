#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatMetrics/operator_frontier.go \
  hat/hatMetrics/mz043_operator_frontier_test.go \
  hat/hatMetrics/mz043_operator_frontier_benchmark_test.go \
  hat/hatCache/operator_frontier_monitoring.go \
  hat/hatCache/mz043_operator_frontier_monitoring_test.go \
  hat/hatCache/mz043_operator_frontier_monitoring_benchmark_test.go \
  hat/hatCache/monitoring.go
