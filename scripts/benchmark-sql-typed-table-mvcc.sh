#!/usr/bin/env bash
set -eu

go test ./hat/hatSql \
  -run '^$' \
  -bench 'BenchmarkTypedTable(PlainWrite|MVCCWrite|PlainRows|MVCCSnapshotRows)$' \
  -benchmem \
  -benchtime=1000x \
  -count=5
