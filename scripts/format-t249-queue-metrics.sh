#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/priority_visibility_queue.go \
  hat/hatDataStructure/deduplicating_priority_visibility_queue.go \
  hat/hatDataStructure/retrying_deduplicating_priority_visibility_queue.go \
  hat/hatDataStructure/t249_queue_metrics_test.go \
  hat/hatDataStructure/t249_queue_metrics_benchmark_test.go
