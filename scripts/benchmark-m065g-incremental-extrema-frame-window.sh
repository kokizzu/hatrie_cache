#!/usr/bin/env bash
set -eu

go test ./hat/hatSql \
	-run '^$' \
	-bench '^BenchmarkIncrementalExtremaFrameWindow/(min_full_scan|min_incremental|max_full_scan|max_incremental)$' \
	-benchtime=200ms \
	-count=5
