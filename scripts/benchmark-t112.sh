#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication ./hat/hatCache -run '^$' -bench '^(BenchmarkMetricsObserveQueueTiming|BenchmarkReplicationCommandRequestResidentBytes)$' -benchmem -count=1
