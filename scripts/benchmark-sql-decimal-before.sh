#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQLRowBinaryDecimalStringBaseline(Encode|Decode)$' -benchmem -count=5
