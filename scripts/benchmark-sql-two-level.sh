#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLColumnarTwoLevelGroupAggregate$' -benchmem -benchtime "${BENCHTIME:-1s}" -count=1
