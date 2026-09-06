#!/bin/sh
set -eu

printf '%s\n' 'Source frontier implementation and monitoring integration:'
rg -n -C 8 'SourceFrontier|source_frontier|SourceFrontierObserved' \
	hat/hatMetrics/source_frontier.go \
	hat/hatCache/monitoring.go \
	hat/hatCache/source_frontier_monitoring.go
printf '%s\n' 'Delivery target and script:'
rg -n -C 4 '^deliver-source-frontier-monitoring:|^format-source-frontier-monitoring:|^verify-source-frontier-monitoring:' Makefile
sed -n '1,260p' scripts/deliver-source-frontier-monitoring.sh
