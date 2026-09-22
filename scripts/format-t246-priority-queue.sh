#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/priority_visibility_queue.go hat/hatDataStructure/priority_visibility_queue_test.go hat/hatDataStructure/t246_priority_visibility_queue_test.go hat/hatDataStructure/t246_priority_visibility_queue_benchmark_test.go
