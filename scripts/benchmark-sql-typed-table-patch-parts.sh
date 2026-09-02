#!/bin/sh
set -eu

go test ./hat/hatSql \
  -run '^$' \
  -bench '^BenchmarkTypedTable(DeleteReinsert|RowsAfterHalfDeletes|PatchCompaction)$' \
  -benchmem \
  -benchtime=100ms \
  -count=5
