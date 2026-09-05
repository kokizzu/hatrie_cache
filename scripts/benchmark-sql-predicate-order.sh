#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLColumnarNumericPredicateOrder$' -benchmem -count=5
