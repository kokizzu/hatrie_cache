#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkExecuteSQLQueryColumnarTopN(MultiOrder|SegmentPruning)?$' -benchmem -count=5
