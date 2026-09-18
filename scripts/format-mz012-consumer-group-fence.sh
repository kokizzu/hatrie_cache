#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/mz012_consumer_group_fence.go \
  hat/hatPipeline/mz012_consumer_group_fence_test.go
