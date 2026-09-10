#!/usr/bin/env bash
set -eu

go test ./hat/hatSql \
	-run '^$' \
	-bench '^BenchmarkIncrementalFrameWindow/(count_full_scan|count_incremental|sum_full_scan|sum_incremental)$' \
	-benchtime=200ms \
	-count=5
