#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQLRowBinaryEnumStringBaseline(Encode|Decode)$' -benchmem -count=5
