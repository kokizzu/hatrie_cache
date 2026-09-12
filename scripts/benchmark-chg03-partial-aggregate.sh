#!/usr/bin/env sh
set -eu

printf '%s\n' 'running CH-G03 partial aggregate state benchmark'
go test -run '^$' -bench '^BenchmarkSQLPartialAggregateStateCodec$' -benchmem -count=5 ./hat/hatSql
