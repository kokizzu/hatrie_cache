#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLLogicalBatchShortCircuit$' -benchmem -count=5
