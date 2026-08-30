#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarStringSource$' -benchmem -count=3
