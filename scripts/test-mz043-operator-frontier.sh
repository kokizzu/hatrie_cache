#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMetrics ./hat/hatCache -run '^Test(OperatorFrontierRegistry|MonitoringPrometheusMetricsExposeOperatorFrontier|MonitoringOperatorFrontierMetricsAreOptIn)'
