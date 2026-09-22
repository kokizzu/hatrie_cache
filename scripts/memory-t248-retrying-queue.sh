#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
	-run '^TestT248RetryingDeduplicatingPriorityVisibilityQueueReportsActiveHeap$' \
	-count=3 \
	-v
