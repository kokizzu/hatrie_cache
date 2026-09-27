#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSchema/rolling_schema.go \
  hat/hatSchema/c154g_durable_rollout_test.go \
  hat/hatSchema/c154g_rollout_benchmark_test.go
