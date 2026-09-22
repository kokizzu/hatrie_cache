#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestM243|TestTypedTableAggregateArrangementsExposeMemoryAndFreshnessStats' -count=1
