#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLIndexFloatValueKey$' -benchmem -count=3
