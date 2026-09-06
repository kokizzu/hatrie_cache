#!/bin/sh
set -eu

printf '%s\n' 'Source frontier implementation and monitoring integration:'
rg -n -C 8 'SourceFrontier|source_frontier|SourceFrontierObserved|OperatorMemory|operator_memory' \
	hat/hatMetrics/source_frontier.go \
	hat/hatMetrics/operator_memory.go \
	hat/hatCache/monitoring.go \
	hat/hatCache/source_frontier_monitoring.go
