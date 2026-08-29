#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLExternalSortParallelMerge$' -benchmem -count=1
