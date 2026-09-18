#!/bin/sh
set -eu

go test ./hat/hatCache \
	-run '^$' \
	-bench '^BenchmarkTR034SQLTransactionSavepoint$' \
	-benchmem \
	-benchtime=10x \
	-count=5
