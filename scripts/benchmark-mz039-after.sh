#!/bin/sh
set -eu

go test ./hat/hatSql \
	-run '^$' \
	-bench '^BenchmarkMZ039SQLDataflow(Baseline|YieldEvery64|YieldEvery1024)$' \
	-benchmem \
	-benchtime=200ms \
	-count=5
