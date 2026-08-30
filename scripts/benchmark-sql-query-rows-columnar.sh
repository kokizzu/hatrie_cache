#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLQueryRowsColumnarSimpleFilter$' -benchmem -count=5
