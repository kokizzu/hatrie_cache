#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLColumnarVectorGroupAggregate$' -benchtime="${BENCHTIME:-100ms}" -count=5 -benchmem
