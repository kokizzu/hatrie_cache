#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarNumericAggregateConjunction$' -benchmem -count=5
