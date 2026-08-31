#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkExecuteSQLQueryMaterializedTopN$' -benchmem -count=5
