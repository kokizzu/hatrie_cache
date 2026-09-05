#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLPreparedQueryCache' -benchmem -count=1
