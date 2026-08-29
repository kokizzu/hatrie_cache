#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarNumericConjunction$' -benchmem -count=5
