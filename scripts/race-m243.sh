#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestM243|TestTypedTableAggregateArrangementsExposeMemoryAndFreshnessStats' -count=1
