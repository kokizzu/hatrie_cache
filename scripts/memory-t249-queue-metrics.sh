#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestT249PriorityVisibilityQueueMetricsRetainedHeap$' -v -count=3
