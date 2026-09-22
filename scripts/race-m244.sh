#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestM244|TestTypedTableAggregateArrangementsExposeMemoryAndFreshnessStats' -count=1
