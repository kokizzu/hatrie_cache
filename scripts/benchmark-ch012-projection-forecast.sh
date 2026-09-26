#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH012ProjectionAdvisor(DefaultFeedback|WorkloadRecording|ForecastWorkload|CostBased)$' -benchmem -count=5
