#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure/delay_queue.go ./hat/hatDataStructure/dead_letter_queue.go ./hat/hatDataStructure/queue_stats.go ./hat/hatDataStructure/t249_queue_stats_benchmark_test.go -run '^$' -bench '^BenchmarkT249' -benchmem -count=5
