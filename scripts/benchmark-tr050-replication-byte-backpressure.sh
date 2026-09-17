#!/bin/sh
set -eu

go test ./hat/hatCache \
	-run '^$' \
	-bench '^BenchmarkTR050ReplicationByteBudget' \
	-benchmem \
	-benchtime=200ms \
	-count=3
