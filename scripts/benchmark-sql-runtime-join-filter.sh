#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRuntimeJoinFilter$' -benchmem -count=5
