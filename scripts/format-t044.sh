#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatMetrics/t044_heap_fragmentation.go hat/hatMetrics/t044_heap_fragmentation_test.go hat/hatMetrics/t044_heap_fragmentation_benchmark_test.go
