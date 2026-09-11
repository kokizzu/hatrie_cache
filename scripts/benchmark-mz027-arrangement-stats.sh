#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^(BenchmarkTypedTableAggregateArrangements|BenchmarkMZ027ArrangementStats)$' -benchmem -benchtime=1s -count=5
