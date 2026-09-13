#!/bin/sh
set -eu

go test ./hat/hatSql \
	-run '^$' \
	-bench '^BenchmarkMZ039SQLDataflowBaseline$' \
	-benchmem \
	-benchtime=200ms \
	-count=5
