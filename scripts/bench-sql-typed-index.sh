#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLTypedInt64IndexRebuild$' -benchmem -count=1
