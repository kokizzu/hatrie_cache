#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLConstantFolding$' -benchmem -count="${BENCHCOUNT:-5}" -benchtime="${BENCHTIME:-1s}"
