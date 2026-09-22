#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/frontier_retention.go \
  hat/hatPipeline/frontier_retention_policy.go \
  hat/hatPipeline/m246_frontier_retention_policy_test.go \
  hat/hatPipeline/m246_frontier_retention_policy_benchmark_test.go
