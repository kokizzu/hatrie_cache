#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQLRowBinaryEnum(StringBaseline|Typed)(Encode|Decode)$' -benchmem -count=5
