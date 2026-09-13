#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQLRowBinaryEnum(StringBaseline|Typed)(Serial|Parallel)Decode' -benchmem -count=3
