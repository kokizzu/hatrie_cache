#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLQueryRowsSimpleFilter$' -benchmem -count=5
