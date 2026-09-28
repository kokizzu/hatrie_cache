#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/t247_dedup_queue.go \
  hat/hatDataStructure/t247_dedup_queue_test.go \
  hat/hatDataStructure/t247_dedup_queue_benchmark_test.go
