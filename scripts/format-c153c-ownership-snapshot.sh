#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/c153c_partition_ownership_snapshot.go \
  hat/hatPipeline/c153c_partition_ownership_snapshot_test.go \
  hat/hatPipeline/c153c_partition_ownership_snapshot_baseline_benchmark_test.go
