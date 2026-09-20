#!/usr/bin/env bash
set -eu

go test ./hat/hatSql \
  -run '^$' \
  -bench '^BenchmarkM065afMutableRangeAggregateBatchedSamePosition$' \
  -benchtime=100x \
  -count=5 \
  -benchmem \
  -v
