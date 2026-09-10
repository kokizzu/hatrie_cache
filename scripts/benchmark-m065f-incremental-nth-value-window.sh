#!/usr/bin/env bash
set -eu

go test ./hat/hatSql \
	-run '^$' \
	-bench '^BenchmarkIncrementalNthValueWindow/(full_scan|incremental)$' \
	-benchtime=200ms \
	-count=5
