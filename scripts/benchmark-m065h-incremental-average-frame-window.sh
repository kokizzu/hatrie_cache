#!/usr/bin/env bash
set -eu

go test ./hat/hatSql \
    -run '^$' \
    -bench '^BenchmarkIncrementalAverageFrameWindow/(full_scan|incremental)$' \
    -benchtime=200ms \
    -count=5
