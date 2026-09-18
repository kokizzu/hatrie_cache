#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/mz011_partition_offset_frontier.go \
  hat/hatPipeline/mz011_partition_offset_frontier_benchmark_test.go \
  hat/hatPipeline/mz011_partition_offset_frontier_test.go
