#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatStorage/compaction_control.go \
  hat/hatStorage/chu35_optimize_control_test.go \
  hat/hatStorage/chu35_optimize_control_benchmark_test.go
