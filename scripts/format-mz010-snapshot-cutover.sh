#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/mz010_snapshot_cutover.go \
  hat/hatPipeline/mz010_snapshot_cutover_test.go \
  hat/hatPipeline/mz010_snapshot_cutover_benchmark_test.go
