#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestM244|TestTypedTableAggregateArrangementsExposeMemoryAndFreshnessStats' -count=1
