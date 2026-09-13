#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQLRowBinaryEnum(StringBaseline|Typed)Decode$' -benchmem -count=3
