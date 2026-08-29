#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLTypedIndexBaseline$' -benchmem -count=1
