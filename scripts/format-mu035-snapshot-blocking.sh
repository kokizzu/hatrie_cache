#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/mz010_snapshot_cutover.go \
  hat/hatPipeline/mu035_snapshot_blocking_test.go \
  hat/hatPipeline/mu035_snapshot_blocking_benchmark_test.go
