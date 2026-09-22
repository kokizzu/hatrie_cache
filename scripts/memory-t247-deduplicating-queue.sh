#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
	-run '^TestT247DeduplicatingPriorityVisibilityQueueReportsResidentHeap$' \
	-count=3 \
	-v
