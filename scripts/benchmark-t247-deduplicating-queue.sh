#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
	-run '^$' \
	-bench '^BenchmarkT247(PriorityVisibilityQueueLeaseAck|DeduplicatingPriorityVisibilityQueueLeaseAck)' \
	-benchmem \
	-count=5
