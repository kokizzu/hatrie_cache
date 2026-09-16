#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/mz020_resizable_scheduler.go \
  hat/hatPipeline/mz020_resizable_scheduler_test.go \
  hat/hatPipeline/mz020_resizable_scheduler_benchmark_test.go
