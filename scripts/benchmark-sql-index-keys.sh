#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLIndexValueKey$' -benchmem -count=1
