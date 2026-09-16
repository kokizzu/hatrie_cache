#!/bin/sh
set -eu

GOMAXPROCS=1 go test ./hat/hatSql \
	-run '^$' \
	-bench '^BenchmarkSQLSubqueryResultCache' \
	-benchmem \
	-benchtime=1s \
	-count=5
