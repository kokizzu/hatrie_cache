#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkResultCacheHit' -benchmem -count=5
