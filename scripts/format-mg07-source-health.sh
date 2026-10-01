#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatMetrics/source_health.go \
  hat/hatMetrics/source_health_test.go \
  hat/hatMetrics/source_health_unknown_test.go \
  hat/hatMetrics/source_health_benchmark_test.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/source_health_monitoring.go \
  hat/hatCache/source_health_monitoring_test.go
