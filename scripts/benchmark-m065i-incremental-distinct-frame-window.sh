#!/usr/bin/env bash
set -eu

go test ./hat/hatSql \
	-run '^$' \
	-bench '^BenchmarkIncrementalDistinctFrameWindow/(full_scan|incremental)$' \
	-benchmem \
	-benchtime=200ms \
	-count=5
