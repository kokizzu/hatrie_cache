#!/usr/bin/env bash
set -eu

gofmt -w hat/hatPipeline/frontier_retention.go hat/hatPipeline/mz04_compaction_metrics_test.go hat/hatPipeline/mz04_compaction_metrics_benchmark_test.go
