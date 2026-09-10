#!/usr/bin/env bash
set -eu

go test ./hat/hatSql \
	-run '^$' \
	-bench '^BenchmarkIncrementalBoundaryWindow/(first_full_scan|first_incremental|last_full_scan|last_incremental)$' \
	-benchtime=200ms \
	-count=5
