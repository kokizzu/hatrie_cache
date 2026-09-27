#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatPipeline/compaction_coalescing.go \
	hat/hatPipeline/frontier_compaction_scheduler.go \
	hat/hatPipeline/mz004_compaction_coalescing_benchmark_test.go \
	hat/hatPipeline/mz004_compaction_coalescing_test.go
