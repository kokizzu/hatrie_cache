#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication ./hat/hatCache -run 'TestMetricsRecordsQueueTiming|TestReplicationJobEstimatedBytesIncludesRestoredPayload|TestHTTPReplicatorAsyncQueuesMaterializedPayload|TestHTTPReplicatorAsyncReportsFullQueue|TestMonitoringReplicationPauseAndResume' -count=1
