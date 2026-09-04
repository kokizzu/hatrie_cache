#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication ./hat/hatCache -run 'TestMetricsRecordsWireBytesByEncoding|TestHTTPReplicatorRecordsWireMetrics' -count=1
