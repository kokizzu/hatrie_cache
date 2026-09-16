#!/bin/sh
set -eu

GOMAXPROCS=1 go test ./hat/hatSql \
	-run '^$' \
	-bench '^BenchmarkC227' \
	-benchmem \
	-benchtime=250ms \
	-count=5
