#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatDataStructure/deduplicating_priority_visibility_queue.go \
	hat/hatDataStructure/t247_deduplicating_priority_visibility_queue_test.go
if [[ -f hat/hatDataStructure/t247_deduplicating_priority_visibility_queue_benchmark_test.go ]]; then
	gofmt -w hat/hatDataStructure/t247_deduplicating_priority_visibility_queue_benchmark_test.go
fi
