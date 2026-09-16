#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatPipeline/mz007_frontier_backpressure.go \
	hat/hatPipeline/mz007_frontier_backpressure_test.go \
	hat/hatPipeline/mz007_frontier_backpressure_benchmark_test.go
