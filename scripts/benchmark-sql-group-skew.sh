#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLGroupSkewGuard$' -benchmem -count=3
