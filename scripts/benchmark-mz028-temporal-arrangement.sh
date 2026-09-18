#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkMZ028(BaselineTemporalTableOutOfOrderUpsert|BaselineTemporalTableAsOf|IntervalArrangementUpsert|IntervalArrangementAt|BaselineLinearIntervalAt|IntervalArrangementLargeAt)$' -benchmem -benchtime=500ms -count=5
