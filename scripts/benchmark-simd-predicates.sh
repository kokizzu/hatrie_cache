#!/bin/sh
set -eu

go test ./hat/hatPredicate \
	-run '^$' \
	-bench '^BenchmarkMatch(Int64|String)$' \
	-benchmem \
	-count=5
