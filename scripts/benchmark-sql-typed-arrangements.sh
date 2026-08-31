#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableAggregateArrangements$' -benchmem -count=5
