#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLSecondaryIndexSource$' -benchmem -count=1
