#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMonitoring ./hat/hatCache -run 'Test(ReadMemoryReport|MonitoringMemory|MonitoringOpenAPI)' -count=1
