#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
	-run '^$' \
	-bench 'Benchmark(PriorityVisibilityQueueLeaseAck|VisibilityQueueLeaseAckBaseline|PriorityVisibilityQueueMarshal|PriorityVisibilityQueueJSONMarshal|PriorityVisibilityQueueBinaryValue)$' \
	-benchmem \
	-count=5
