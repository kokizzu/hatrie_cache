#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMetrics ./hat/hatCache \
  -run '^$' \
  -bench '^(BenchmarkMZ043(Source|Operator)FrontierRegistry|BenchmarkMonitoringMetrics)' \
  -benchmem \
  -count=5
