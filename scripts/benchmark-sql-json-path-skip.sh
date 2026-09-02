#!/bin/sh
set -eu

go test ./hat/hatCache \
  -run '^$' \
  -bench '^BenchmarkSQLJSONPathSkipEquality$' \
  -benchmem \
  -benchtime=100ms \
  -count=5
