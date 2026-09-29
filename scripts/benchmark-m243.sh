#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM243TypedTableAggregateArrangementsStats$' -benchmem -cpu=1 -count=7
