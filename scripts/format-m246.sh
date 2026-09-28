#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatPipeline/frontier_retention.go \
	hat/hatPipeline/m246_history_retention_policy_test.go \
	hat/hatPipeline/m246_history_retention_legacy_benchmark_test.go \
	hat/hatPipeline/m246_history_retention_policy_benchmark_test.go
