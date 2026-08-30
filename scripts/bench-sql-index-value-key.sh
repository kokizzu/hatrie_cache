#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLIndex(Float|Integer)ValueKey$' -benchmem -count=3
