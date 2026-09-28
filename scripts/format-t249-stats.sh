#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/queue_stats.go \
  hat/hatDataStructure/t249_queue_stats_test.go \
  hat/hatDataStructure/t249_queue_stats_benchmark_test.go
