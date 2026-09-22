#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatDataStructure/retrying_deduplicating_priority_visibility_queue.go \
	hat/hatDataStructure/t248_retrying_deduplicating_priority_visibility_queue_test.go \
	hat/hatDataStructure/t248_retrying_deduplicating_priority_visibility_queue_benchmark_test.go
