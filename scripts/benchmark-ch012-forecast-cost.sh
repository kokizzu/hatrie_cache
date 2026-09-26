#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH012ProjectionAdvisorForecastCostBased$' -benchmem -count=5
