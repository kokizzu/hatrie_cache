#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench 'BenchmarkDelayQueuePopReadyC215|BenchmarkDelayQueuePushPop|BenchmarkVisibilityQueueLeaseAck' -benchmem -count="${BENCH_COUNT:-5}"
