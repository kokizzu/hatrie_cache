#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLIndexFreshnessIdentity$' -benchmem -count=1
