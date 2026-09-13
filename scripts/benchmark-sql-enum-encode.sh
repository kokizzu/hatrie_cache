#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQLRowBinaryEnum(StringBaseline|Typed)Encode$' -benchmem -count=3
