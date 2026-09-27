#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
    hat/hatPipeline/frontier_compaction_scheduler.go \
	 hat/hatPipeline/mz004_compaction_priority_benchmark_test.go \
    hat/hatPipeline/mz004_compaction_priority_test.go \
    hat/hatPipeline/priority_scheduler.go
