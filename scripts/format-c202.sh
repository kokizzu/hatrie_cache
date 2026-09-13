#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPipeline/partitioned_async_batcher.go hat/hatPipeline/c202_partitioned_async_batcher_test.go hat/hatPipeline/c202_partitioned_async_batcher_benchmark_test.go
