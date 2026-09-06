#!/bin/sh
set -eu

go test ./hat/hatSql \
	-run '^$' \
	-bench '^BenchmarkFillSQLRows$' \
	-benchmem \
	-count=5
