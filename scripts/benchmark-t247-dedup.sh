#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure/t247_dedup_queue.go ./hat/hatDataStructure/t247_dedup_queue_benchmark_test.go -run '^$' -bench '^BenchmarkT247' -benchmem -count=5
