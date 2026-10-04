#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/consumer_group_queue.go \
  hat/hatPipeline/consumer_group_queue_test.go \
  hat/hatPipeline/consumer_group_queue_benchmark_test.go
