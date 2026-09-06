#!/bin/sh
set -eu

go test ./hat/hatSql \
	-run '^$' \
	-bench '^BenchmarkSQLAggregateCombinatorRegistryNewState$' \
	-benchmem \
	-count=5
