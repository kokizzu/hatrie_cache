#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkExecuteSQLQueryColumnarTopN$' -benchmem -count=5
