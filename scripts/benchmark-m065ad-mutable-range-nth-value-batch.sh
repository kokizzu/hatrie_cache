#!/usr/bin/env bash
set -eu

go test ./hat/hatSql \
  -run '^$' \
  -bench '^BenchmarkM065adMutableRangeNthValueBatchedSamePosition$' \
  -benchtime=100x \
  -count=5 \
  -benchmem \
  -v
