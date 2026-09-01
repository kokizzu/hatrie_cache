#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTable' -benchmem -count=3
