#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/frontier_compaction_policy.go \
  hat/hatPipeline/frontier_compaction_scheduler.go \
  hat/hatPipeline/mz004_compaction_policy_test.go \
  hat/hatPipeline/mz004_compaction_policy_benchmark_test.go
