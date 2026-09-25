#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/mz001_durable_persist_shard.go \
  hat/hatPipeline/mz001_durable_persist_shard_test.go \
  hat/hatPipeline/mz001_durable_persist_shard_baseline_benchmark_test.go \
  hat/hatPipeline/mz001_durable_persist_shard_benchmark_test.go
