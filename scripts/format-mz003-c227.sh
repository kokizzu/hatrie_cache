#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/frontier_compaction_scheduler.go \
  hat/hatPipeline/frontier_retention.go \
  hat/hatPipeline/mz003_frontier_compaction_scheduler_test.go
