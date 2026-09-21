#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/mz046_frontier_cancellation.go \
  hat/hatPipeline/mz046_frontier_cancellation_test.go \
  hat/hatPipeline/mz046_frontier_cancellation_benchmark_test.go \
  hat/hatPipeline/mz046_frontier_cancellation_baseline_benchmark_test.go
