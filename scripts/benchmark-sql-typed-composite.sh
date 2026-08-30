#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLTypedInt64CompositePrefixRange$' -benchmem -count=5
