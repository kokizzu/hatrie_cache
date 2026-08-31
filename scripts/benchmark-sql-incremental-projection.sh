#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkIncrementalProjectionCoalescedRefresh$' -benchmem -count=5
