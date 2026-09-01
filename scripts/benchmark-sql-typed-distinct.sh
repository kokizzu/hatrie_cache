#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableAggregateCountDistinct$' -benchmem -count=5
