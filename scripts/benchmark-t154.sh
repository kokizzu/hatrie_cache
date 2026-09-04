#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench 'BenchmarkMonitoringCommandSlowCapture' -benchmem -count=3
