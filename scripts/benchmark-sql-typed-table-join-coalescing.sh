#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableJoinCoalescing$' -benchmem -benchtime=1x -count=5
