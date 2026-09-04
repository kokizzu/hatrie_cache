#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'TestHTTPReplicatorAsyncPauseAndResume|TestMonitoringReplicationPauseAndResume|TestHTTPReplicatorAsyncPauseRequiresQueue' -count=1
