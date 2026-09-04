#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication ./hat/hatCache -run 'TestMetricsRecordsWireBytesByEncoding|TestHTTPReplicatorRecordsWireMetrics|TestPrometheusReplicationWireMetrics' -count=1
