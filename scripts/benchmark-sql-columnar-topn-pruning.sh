#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkExecuteSQLQueryColumnarTopNSegmentPruning$' -benchmem -count=5
