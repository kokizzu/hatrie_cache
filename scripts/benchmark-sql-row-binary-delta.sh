#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRowBinaryDelta$' -benchmem -count=5
