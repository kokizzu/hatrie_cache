#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnar(NumericFilter|Scan)$' -benchmem -count=1
