#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLPreparedQueryCacheHit$' -benchmem -count=1
