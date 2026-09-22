#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^(BenchmarkPriorityVisibilityQueueLeaseAck|BenchmarkVisibilityQueueLeaseAckBaseline|BenchmarkT246PriorityVisibilityQueue)' -benchmem -count=3
