#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQL(ExternalSort|SpillGroup)ParallelMerge$' -benchmem -count=1
