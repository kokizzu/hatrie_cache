#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLApproximateAggregates$' -benchmem -count=5
